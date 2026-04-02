'use strict';

/**
 * zillow-historical.js — one-time backfill script
 *
 * For each row in "Listing Archive" that lacks a list price (col E) and ZPID (col P):
 *   1. Searches Zillow via zillow-detail-scraper using the address
 *   2. Finds the listing cycle that started within 90 days after the shoot date
 *   3. Writes: E=listPrice, F=zillowUrl, G=listDate, H=soldPrice, I=soldDate,
 *              K=daysOnMarket, P=zpid
 *
 * Usage:
 *   node --env-file=.env zillow-historical.js
 */

const fetch      = require('node-fetch');
const { google } = require('googleapis');

const SHEET_ID    = process.env.GOOGLE_SHEET_ID;
const ARCHIVE_TAB = 'Listing Archive';
const APIFY_TOKEN = process.env.APIFY_TOKEN;
const APIFY_BASE  = 'https://api.apify.com/v2';
const APIFY_POLL_MS     = 6_000;
const APIFY_MAX_WAIT_MS = 300_000;

// Archive tab column indices (0-based)
const COL = {
  address:      0,  // A
  agent:        1,  // B
  shootDate:    2,  // C
  pkg:          3,  // D
  listPrice:    4,  // E
  zillowUrl:    5,  // F
  listDate:     6,  // G
  soldPrice:    7,  // H
  soldDate:     8,  // I
  daysOnMarket: 10, // K
  zpid:         15, // P
};

// ─── Google Sheets auth ───────────────────────────────────────────────────────

function getSheetsClient() {
  const auth = new google.auth.JWT({
    email:  process.env.GOOGLE_CLIENT_EMAIL,
    key:    (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// ─── Apify ────────────────────────────────────────────────────────────────────

async function runApifyActor(actorId, input) {
  const safeId   = actorId.replace('/', '~');
  const startRes = await fetch(
    `${APIFY_BASE}/acts/${safeId}/runs?token=${APIFY_TOKEN}&memory=512`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(input) }
  );
  if (!startRes.ok) throw new Error(`Failed to start ${actorId} [${startRes.status}]: ${await startRes.text()}`);
  const { data: run } = await startRes.json();
  const runId = run.id;

  const deadline = Date.now() + APIFY_MAX_WAIT_MS;
  let status = run.status;
  while (!['SUCCEEDED', 'FAILED', 'TIMED-OUT', 'ABORTED'].includes(status)) {
    if (Date.now() > deadline) throw new Error(`Actor ${actorId} run ${runId} timed out`);
    await new Promise(r => setTimeout(r, APIFY_POLL_MS));
    const poll = await fetch(`${APIFY_BASE}/actor-runs/${runId}?token=${APIFY_TOKEN}`);
    status = (await poll.json()).data.status;
  }
  if (status !== 'SUCCEEDED') throw new Error(`Actor ${actorId} run ${runId}: ${status}`);

  const itemsRes = await fetch(`${APIFY_BASE}/actor-runs/${runId}/dataset/items?token=${APIFY_TOKEN}&limit=5`);
  if (!itemsRes.ok) throw new Error(`Failed to fetch dataset for run ${runId}`);
  return itemsRes.json();
}

// ─── Price history analysis ───────────────────────────────────────────────────

// Parse a date value from Zillow price history (timestamp ms or date string).
function parseHistoryDate(val) {
  if (!val) return null;
  const n = Number(val);
  const d = !isNaN(n) && n > 1e10
    ? new Date(n)
    : new Date(val);
  return isNaN(d.getTime()) ? null : d;
}

function toYMD(d) {
  if (!d) return '';
  return d.toISOString().slice(0, 10);
}

// Find the listing cycle in priceHistory that began within 90 days after shootDate.
// Returns { listPrice, listDate, soldPrice, soldDate, daysOnMarket } or null.
function findListingCycle(priceHistory, shootDate) {
  if (!Array.isArray(priceHistory) || !shootDate) return null;

  const shoot    = new Date(shootDate);
  const earliest = shoot;
  const latest   = new Date(shoot.getTime() + 90 * 24 * 60 * 60 * 1000);

  // Sort oldest first
  const sorted = [...priceHistory].sort((a, b) => {
    const da = parseHistoryDate(a.time || a.date);
    const db = parseHistoryDate(b.time || b.date);
    return (da?.getTime() ?? 0) - (db?.getTime() ?? 0);
  });

  // Find the first "Listed for sale" event within the window
  const listEvent = sorted.find(e => {
    if (!/listed\s+for\s+sale/i.test(e.event || '')) return false;
    const d = parseHistoryDate(e.time || e.date);
    return d && d >= earliest && d <= latest;
  });

  if (!listEvent) return null;

  const listDate  = parseHistoryDate(listEvent.time || listEvent.date);
  const listPrice = listEvent.price ?? null;

  // Find the subsequent "Sold" event after the list date
  const soldEvent = sorted.find(e => {
    if (!/sold/i.test(e.event || '')) return false;
    const d = parseHistoryDate(e.time || e.date);
    return d && listDate && d > listDate;
  });

  const soldDate  = soldEvent ? parseHistoryDate(soldEvent.time || soldEvent.date) : null;
  const soldPrice = soldEvent?.price ?? null;

  // Days on market: list → sold, or list → today if still active
  const endDate   = soldDate || new Date();
  const dom       = listDate ? Math.round((endDate - listDate) / (1000 * 60 * 60 * 24)) : null;

  return {
    listPrice,
    listDate:    toYMD(listDate),
    soldPrice,
    soldDate:    toYMD(soldDate),
    daysOnMarket: dom,
  };
}

// ─── Sheet helpers ────────────────────────────────────────────────────────────

async function readArchiveRows(sheets) {
  const res  = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `'${ARCHIVE_TAB}'!A:P`,
  });
  const rows = res.data.values || [];
  const result = [];
  // Skip rows 1 (legend) and 2 (headers) — start at index 2 (row 3)
  for (let i = 2; i < rows.length; i++) {
    const row       = rows[i];
    const address   = (row[COL.address]   || '').trim();
    const shootDate = (row[COL.shootDate] || '').trim();
    const listPrice = (row[COL.listPrice] || '').trim();
    const zpid      = (row[COL.zpid]      || '').trim();
    if (!address) continue;
    result.push({ address, shootDate, listPrice, zpid, rowIndex: i + 1 });
  }
  return result;
}

async function writeArchiveRow(sheets, rowIndex, data) {
  const col = n => String.fromCharCode(65 + n);
  const updates = [
    [COL.listPrice,    data.listPrice    ?? ''],
    [COL.zillowUrl,    data.zillowUrl    ?? ''],
    [COL.listDate,     data.listDate     ?? ''],
    [COL.soldPrice,    data.soldPrice    ?? ''],
    [COL.soldDate,     data.soldDate     ?? ''],
    [COL.daysOnMarket, data.daysOnMarket ?? ''],
    [COL.zpid,         data.zpid         ?? ''],
  ].map(([c, value]) => ({
    range:  `'${ARCHIVE_TAB}'!${col(c)}${rowIndex}`,
    values: [[value]],
  }));

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody:   { valueInputOption: 'USER_ENTERED', data: updates },
  });
}

// ─── Zillow lookup ────────────────────────────────────────────────────────────

// Step 1: use zillow-scraper to search by address and get the ZPID
async function findZpidByAddress(address) {
  const encoded = encodeURIComponent(address);
  const url     = `https://www.zillow.com/homes/${encoded}_rb/`;
  const items   = await runApifyActor('maxcopell/zillow-scraper', {
    searchUrls: [{ url }],
    maxItems:   5,
  });
  if (!items?.length) return null;

  // Match by street address
  const street = address.split(',')[0].trim().toLowerCase();
  const match  = items.find(i => {
    const s = (i.addressStreet || i.address || '').toLowerCase();
    return s.includes(street) || street.includes(s.split(',')[0].trim());
  }) || items[0];

  return match?.zpid ? String(match.zpid) : null;
}

// Step 2: use zillow-detail-scraper with the ZPID URL to get full price history
async function fetchDetailByZpid(zpid) {
  const url   = `https://www.zillow.com/homedetails/home/${zpid}_zpid/`;
  const items = await runApifyActor('maxcopell/zillow-detail-scraper', {
    startUrls: [{ url }],
    maxItems:  1,
  });
  return items?.[0] ?? null;
}

async function lookupZillow(address) {
  const zpid = await findZpidByAddress(address);
  if (!zpid) return null;
  const detail = await fetchDetailByZpid(zpid);
  if (detail) detail._zpid = zpid; // carry ZPID through
  return detail;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const sheets = getSheetsClient();

  console.log('Reading Listing Archive...');
  const rows = await readArchiveRows(sheets);
  console.log(`Found ${rows.length} row(s) total.`);

  const toProcess = rows.filter(r => !r.zpid && !r.listPrice);
  console.log(`${toProcess.length} row(s) need processing (no ZPID and no list price).\n`);

  let processed = 0, skipped = 0, failed = 0;

  for (const { address, shootDate, rowIndex } of toProcess) {
    console.log(`[${++processed}/${toProcess.length}] Row ${rowIndex}: ${address}`);

    try {
      const detail = await lookupZillow(address);
      if (!detail) {
        console.warn(`  No Zillow result found — skipping`);
        skipped++;
        continue;
      }

      const zpid      = String(detail._zpid || detail.zpid || detail.hdpData?.homeInfo?.zpid || '');
      const zillowUrl = detail.hdpUrl
        ? `https://www.zillow.com${detail.hdpUrl}`
        : (detail.detailUrl ?? '');

      const priceHistory = detail.priceHistory || detail.hdpData?.homeInfo?.priceHistory || [];
      const cycle = findListingCycle(priceHistory, shootDate);

      if (!cycle) {
        console.warn(`  No listing cycle found within 90 days after ${shootDate || 'unknown shoot date'} — skipping`);
        // Still write ZPID and URL so we know we checked this property
        if (zpid) {
          await writeArchiveRow(sheets, rowIndex, { zpid, zillowUrl, listPrice: '', listDate: '', soldPrice: '', soldDate: '', daysOnMarket: '' });
          console.log(`  ZPID ${zpid} saved for future reference`);
        }
        skipped++;
        continue;
      }

      console.log(`  Listing cycle: listed ${cycle.listDate} @ $${cycle.listPrice}, sold ${cycle.soldDate || 'n/a'} @ $${cycle.soldPrice || 'n/a'}, DOM ${cycle.daysOnMarket}`);

      await writeArchiveRow(sheets, rowIndex, {
        zpid,
        zillowUrl,
        listPrice:    cycle.listPrice,
        listDate:     cycle.listDate,
        soldPrice:    cycle.soldPrice,
        soldDate:     cycle.soldDate,
        daysOnMarket: cycle.daysOnMarket,
      });
      console.log(`  Row ${rowIndex} updated.`);

    } catch (err) {
      console.error(`  Error: ${err.message}`);
      failed++;
    }

    // Rate-limit delay between properties
    if (processed < toProcess.length) {
      await new Promise(r => setTimeout(r, 2_000));
    }
  }

  console.log(`\nDone. Processed ${toProcess.length}: ${toProcess.length - skipped - failed} updated, ${skipped} skipped, ${failed} failed.`);
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});

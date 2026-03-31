'use strict';

const cron = require('node-cron');
const fetch = require('node-fetch');
const { google } = require('googleapis');

// ─── Config ──────────────────────────────────────────────────────────────────

const SHEET_ID       = process.env.GOOGLE_SHEET_ID;
const SHEET_TAB      = 'Listing Tracker';
const APIFY_TOKEN    = process.env.APIFY_TOKEN;
const ZAPIER_WEBHOOK = process.env.ZAPIER_WEBHOOK;

// 0-based column indices  →  sheet letter
const COL = {
  address:      0,  // A
  listPrice:    4,  // E
  zillowUrl:    5,  // F
  listDate:     6,  // G
  soldPrice:    7,  // H
  soldDate:     8,  // I
  daysOnMarket: 9,  // J
  zpid:         10, // K  ← persisted; empty on first run, populated after first match
  marketAvgDom: 11, // L
};

// ZIP → Zillow sold-homes search URL (must include full searchQueryState)
const ZIP_SOLD_URLS = {
  '85718': 'https://www.zillow.com/tucson-az-85718/sold/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-111.14097846728515%2C%22east%22%3A-110.69534553271484%2C%22south%22%3A32.20951945556616%2C%22north%22%3A32.4717400226085%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95056%2C%22regionType%22%3A7%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%2C%22usersSearchTerm%22%3A%2285718%22%7D',
  '85755': 'https://www.zillow.com/tucson-az-85755/sold/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-111.14097846728515%2C%22east%22%3A-110.69534553271484%2C%22south%22%3A32.20951945556616%2C%22north%22%3A32.4717400226085%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95056%2C%22regionType%22%3A7%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%2C%22usersSearchTerm%22%3A%2285755%22%7D',
  '85742': 'https://www.zillow.com/oro-valley-az-85742/sold/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-111.14097846728515%2C%22east%22%3A-110.69534553271484%2C%22south%22%3A32.20951945556616%2C%22north%22%3A32.4717400226085%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95056%2C%22regionType%22%3A7%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%2C%22usersSearchTerm%22%3A%2285742%22%7D',
};

// ─── Google Sheets auth ───────────────────────────────────────────────────────

function getSheetsClient() {
  const auth = new google.auth.JWT({
    email: process.env.GOOGLE_CLIENT_EMAIL,
    key: (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// ─── Apify helpers ────────────────────────────────────────────────────────────

const APIFY_BASE = 'https://api.apify.com/v2';
const APIFY_POLL_MS = 6_000;
const APIFY_MAX_WAIT_MS = 180_000;

async function runApifyActor(actorId, input) {
  const safeId = actorId.replace('/', '~');
  const startRes = await fetch(
    `${APIFY_BASE}/acts/${safeId}/runs?token=${APIFY_TOKEN}&memory=512`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(input) }
  );
  if (!startRes.ok) {
    const text = await startRes.text();
    throw new Error(`Failed to start actor ${actorId} [${startRes.status}]: ${text}`);
  }
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

  const itemsRes = await fetch(
    `${APIFY_BASE}/actor-runs/${runId}/dataset/items?token=${APIFY_TOKEN}&limit=200`
  );
  if (!itemsRes.ok) throw new Error(`Failed to fetch dataset for run ${runId}`);
  return itemsRes.json();
}

// ─── ZPID-based detail lookup (fast path) ────────────────────────────────────

async function fetchDetailByZpid(zpid) {
  const url = `https://www.zillow.com/homedetails/home/${zpid}_zpid/`;
  console.log(`  Fetching detail via ZPID ${zpid}`);
  const items = await runApifyActor('maxcopell/zillow-detail-scraper', {
    startUrls: [{ url }],
    maxItems: 1,
  });
  const item = items?.[0];
  if (!item || item.isValid === false) {
    console.warn(`  Detail scraper returned invalid result for ZPID ${zpid}`);
    return null;
  }
  return item;
}

// ─── ZIP listing cache (address-search fallback) ──────────────────────────────

const zipCache = {};

function buildActiveUrl(soldUrl) {
  const qIdx = soldUrl.indexOf('searchQueryState=');
  if (qIdx === -1) return null;
  const state = JSON.parse(decodeURIComponent(soldUrl.slice(qIdx + 'searchQueryState='.length)));
  // Remove sold-only filter flags
  const { rs, fsba, fsbo, nc, cmsn, auc, fore, ...keepFilters } = state.filterState || {};
  state.filterState = keepFilters;
  const basePath = soldUrl.split('?')[0].replace(/\/sold\/?$/, '/');
  return `${basePath}?searchQueryState=${encodeURIComponent(JSON.stringify(state))}`;
}

async function getZipListings(zip, type) {
  if (!zipCache[zip]) zipCache[zip] = {};
  if (zipCache[zip][type]) return zipCache[zip][type];

  const soldUrl = ZIP_SOLD_URLS[zip];
  if (!soldUrl) { zipCache[zip][type] = []; return []; }

  const url = type === 'sold' ? soldUrl : buildActiveUrl(soldUrl);
  if (!url) { zipCache[zip][type] = []; return []; }

  console.log(`  Fetching ${type} listings for ZIP ${zip}`);
  const items = await runApifyActor('maxcopell/zillow-scraper', {
    searchUrls: [{ url }],
    maxItems: 100,
  });
  zipCache[zip][type] = items || [];
  return zipCache[zip][type];
}

function findInListings(address, listings) {
  const street = address.split(',')[0].trim().toLowerCase();
  return listings.find(item => {
    const s = (item.addressStreet || item.address || '').toLowerCase();
    return s.includes(street) || street.includes(s.split(',')[0]);
  }) || null;
}

// ─── Main listing detail resolver ─────────────────────────────────────────────
//
// Returns { detail, zpid, isNewZpid }
// - If a stored zpid is provided:   use detail scraper directly (fast)
// - If no zpid (first run):         search ZIP listings, extract zpid, flag as new so
//                                   the caller can persist it to the sheet

async function resolveDetail(address, storedZpid) {
  // Fast path: we already have the ZPID
  if (storedZpid) {
    const detail = await fetchDetailByZpid(storedZpid);
    if (detail) return { detail, zpid: storedZpid, isNewZpid: false };
    console.warn(`  ZPID ${storedZpid} lookup failed, falling back to search`);
  }

  // Slow path: search ZIP listings and extract ZPID
  const zip = extractZip(address);
  if (!zip || !ZIP_SOLD_URLS[zip]) {
    console.warn(`  No ZIP URL mapping for ${zip || 'unknown ZIP'}`);
    return { detail: null, zpid: null, isNewZpid: false };
  }

  for (const type of ['sold', 'active']) {
    try {
      const listings = await getZipListings(zip, type);
      const match = findInListings(address, listings);
      if (match?.zpid) {
        const zpid = String(match.zpid);
        console.log(`  Found via ${type} search — ZPID ${zpid} (will be saved)`);
        // Upgrade to full detail using the newly discovered ZPID
        const detail = await fetchDetailByZpid(zpid);
        return { detail: detail || match, zpid, isNewZpid: true };
      }
    } catch (e) {
      console.warn(`  ${type} search error: ${e.message}`);
    }
  }

  console.warn(`  Address not found in any ZIP ${zip} listing`);
  return { detail: null, zpid: null, isNewZpid: false };
}

// ─── Field extraction from zillow-detail-scraper result ──────────────────────

function extractZip(address) {
  const m = address.match(/\b(\d{5})\b/);
  return m ? m[1] : null;
}

// Returns val if it looks like a date string (YYYY-MM-DD or M/D/YYYY), otherwise ''.
// Prevents numeric daysOnZillow values from leaking into date columns.
function safeDate(val) {
  if (!val && val !== 0) return '';
  const s = String(val).trim();
  return (/^\d{4}-\d{2}-\d{2}$/.test(s) || /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)) ? s : '';
}

// Only called for RECENTLY_SOLD listings — never for FOR_SALE.
function getSoldInfoFromPriceHistory(priceHistory) {
  if (!Array.isArray(priceHistory)) return { soldPrice: '', soldDate: '' };
  const sold = [...priceHistory]
    .filter(e => /sold/i.test(e.event || ''))
    .sort((a, b) => (b.time || 0) - (a.time || 0))[0];
  return {
    soldPrice: sold?.price ?? '',
    soldDate:  safeDate(sold?.date),
  };
}

function extractFields(detail) {
  if (!detail) {
    return { listPrice: '', zillowUrl: '', listDate: '', soldPrice: '', soldDate: '', daysOnMarket: '' };
  }

  // zillow-detail-scraper result (has hdpUrl field)
  if ('hdpUrl' in detail) {
    // Only treat as sold when Zillow explicitly marks it RECENTLY_SOLD.
    // FOR_SALE listings always have empty sold fields even if priceHistory
    // contains old "Sold" events from previous ownership.
    const isRecentlySold = detail.homeStatus === 'RECENTLY_SOLD';
    const { soldPrice, soldDate } = isRecentlySold
      ? getSoldInfoFromPriceHistory(detail.priceHistory)
      : { soldPrice: '', soldDate: '' };

    return {
      listPrice:    detail.price        ?? '',
      zillowUrl:    detail.hdpUrl ? `https://www.zillow.com${detail.hdpUrl}` : '',
      listDate:     safeDate(detail.datePostedString),
      soldPrice,
      soldDate,
      daysOnMarket: detail.daysOnZillow ?? '',
    };
  }

  // zillow-scraper search result (fallback when detail-scraper is unavailable)
  const info   = detail.hdpData?.homeInfo || {};
  const isSold = detail.statusType === 'SOLD' || detail.rawHomeStatusCd === 'RecentlySold';
  const ts     = info.dateSold;
  const soldDateRaw = ts
    ? new Date(typeof ts === 'number' && ts > 1e10 ? ts : ts * 1000).toLocaleDateString('en-US')
    : '';

  return {
    listPrice:    info.price ?? detail.unformattedPrice ?? '',
    zillowUrl:    detail.detailUrl ?? '',
    listDate:     '',
    soldPrice:    isSold ? (info.price ?? detail.unformattedPrice ?? '') : '',
    soldDate:     isSold ? safeDate(soldDateRaw) : '',
    daysOnMarket: info.daysOnZillow ?? '',
  };
}

// ─── Market average DOM ───────────────────────────────────────────────────────

async function fetchMarketAvgDom(zip) {
  const sold = await getZipListings(zip, 'sold');
  if (!sold?.length) return null;
  const vals = sold
    .map(i => i.hdpData?.homeInfo?.daysOnZillow ?? null)
    .filter(v => v !== null && !isNaN(Number(v)))
    .map(Number);
  if (!vals.length) return null;
  return Math.round(vals.reduce((a, b) => a + b, 0) / vals.length);
}

// ─── Sheet read/write ─────────────────────────────────────────────────────────

async function readAddresses(sheets) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `'${SHEET_TAB}'!A:K`,  // read through K to include stored ZPIDs
  });
  const rows = res.data.values || [];
  const result = [];
  for (let i = 2; i < rows.length; i++) {
    const addr = (rows[i][COL.address] || '').trim();
    const zpid = (rows[i][COL.zpid]    || '').trim();
    if (addr) result.push({ address: addr, zpid, rowIndex: i + 1 });
  }
  return result;
}

async function updateSheetRow(sheets, rowIndex, data) {
  const col = n => String.fromCharCode(65 + n);
  const updates = [
    [COL.listPrice,    data.listPrice    ?? ''],
    [COL.zillowUrl,    data.zillowUrl    ?? ''],
    [COL.listDate,     data.listDate     ?? ''],
    [COL.soldPrice,    data.soldPrice    ?? ''],
    [COL.soldDate,     data.soldDate     ?? ''],
    [COL.daysOnMarket, data.daysOnMarket ?? ''],
    [COL.zpid,         data.zpid         ?? ''],
    [COL.marketAvgDom, data.marketAvgDom ?? ''],
  ].map(([c, value]) => ({ range: `'${SHEET_TAB}'!${col(c)}${rowIndex}`, values: [[value]] }));

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: { valueInputOption: 'USER_ENTERED', data: updates },
  });
}

// ─── Zapier notification ──────────────────────────────────────────────────────

async function sendToZapier(payload) {
  if (!ZAPIER_WEBHOOK) { console.warn('  ZAPIER_WEBHOOK not set'); return; }
  const res = await fetch(ZAPIER_WEBHOOK, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) console.warn(`  Zapier returned ${res.status}`);
}

// ─── Main job ─────────────────────────────────────────────────────────────────

async function runMonitor() {
  console.log(`[${new Date().toISOString()}] Starting property monitor run`);
  Object.keys(zipCache).forEach(k => delete zipCache[k]); // clear per-run cache

  const sheets = getSheetsClient();
  let addressRows;
  try {
    addressRows = await readAddresses(sheets);
  } catch (err) {
    console.error('Failed to read sheet:', err.message);
    return;
  }
  console.log(`Found ${addressRows.length} addresses to process`);

  for (const { address, zpid: storedZpid, rowIndex } of addressRows) {
    console.log(`\nProcessing [row ${rowIndex}]: ${address}${storedZpid ? ` (ZPID ${storedZpid})` : ' (no ZPID yet)'}`);
    const zip = extractZip(address);

    let detail = null, zpid = storedZpid, isNewZpid = false;
    try {
      ({ detail, zpid, isNewZpid } = await resolveDetail(address, storedZpid));
      if (detail) console.log(`  Detail: ${detail.streetAddress || detail.addressStreet || address} — ${detail.homeStatus || detail.statusType}`);
      else console.warn('  No detail found');
    } catch (err) {
      console.error(`  Detail error: ${err.message}`);
    }

    let marketAvgDom = null;
    if (zip && ZIP_SOLD_URLS[zip]) {
      try {
        marketAvgDom = await fetchMarketAvgDom(zip);
        console.log(`  Market avg DOM (${zip}): ${marketAvgDom}`);
      } catch (err) {
        console.error(`  Market DOM error: ${err.message}`);
      }
    } else {
      console.warn(`  No sold URL for ZIP ${zip}, skipping market DOM`);
    }

    const rowData = { ...extractFields(detail), zpid: zpid || '', marketAvgDom };

    try {
      await updateSheetRow(sheets, rowIndex, rowData);
      const zpidNote = isNewZpid ? ` (ZPID ${zpid} saved)` : '';
      console.log(`  Sheet row ${rowIndex} updated${zpidNote}`);
    } catch (err) {
      console.error(`  Sheet write error: ${err.message}`);
    }

    try {
      await sendToZapier({ address, rowIndex, ...rowData, updatedAt: new Date().toISOString() });
      console.log('  Zapier notified');
    } catch (err) {
      console.error(`  Zapier error: ${err.message}`);
    }
  }

  console.log(`\n[${new Date().toISOString()}] Monitor run complete`);
}

// ─── Scheduler ────────────────────────────────────────────────────────────────

cron.schedule('0 8 * * *', () => {
  runMonitor().catch(err => console.error('Unhandled error:', err));
}, { timezone: 'America/Phoenix' });

console.log('Property monitor started. Scheduled daily at 08:00 America/Phoenix.');

if (process.env.RUN_NOW === 'true') {
  runMonitor().catch(err => console.error('Unhandled error in immediate run:', err));
}

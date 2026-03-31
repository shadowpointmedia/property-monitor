'use strict';

const cron = require('node-cron');
const fetch = require('node-fetch');
const { google } = require('googleapis');

// ─── Config ──────────────────────────────────────────────────────────────────

const SHEET_ID       = process.env.GOOGLE_SHEET_ID;
const SHEET_TAB      = 'Listing Tracker';
const APIFY_TOKEN    = process.env.APIFY_TOKEN;
const ZAPIER_WEBHOOK = process.env.ZAPIER_WEBHOOK;

// Column indices (0-based) in the sheet
const COL = {
  address:      0,  // A
  price:        4,  // E
  zillowUrl:    5,  // F
  listDate:     6,  // G
  soldPrice:    7,  // H
  soldDate:     8,  // I
  daysOnMarket: 9,  // J
  marketAvgDom: 11, // L
};

// ZIP → Zillow sold-homes search URL
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

/**
 * Run an Apify actor and wait for it to finish, then return dataset items.
 * Uses the synchronous run endpoint with a generous timeout.
 */
async function runApifyActor(actorId, input) {
  const url = `https://api.apify.com/v2/acts/${actorId}/run-sync-get-dataset-items` +
              `?token=${APIFY_TOKEN}&timeout=120&memory=256`;

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
    timeout: 150_000,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Apify actor ${actorId} failed [${res.status}]: ${text}`);
  }

  return res.json();
}

// ─── Zillow detail scraper ────────────────────────────────────────────────────

async function fetchListingDetail(address) {
  console.log(`  Fetching Zillow detail for: ${address}`);
  const items = await runApifyActor('maxcopell/zillow-detail-scraper', {
    startUrls: [],
    search: address,
    maxItems: 1,
  });

  if (!items || items.length === 0) return null;
  return items[0];
}

// ─── Market average DOM ───────────────────────────────────────────────────────

function extractZip(address) {
  const m = address.match(/\b(\d{5})\b/);
  return m ? m[1] : null;
}

async function fetchMarketAvgDom(zip) {
  const soldUrl = ZIP_SOLD_URLS[zip];
  if (!soldUrl) {
    console.warn(`  No sold-URL mapping for ZIP ${zip}, skipping market DOM`);
    return null;
  }

  console.log(`  Fetching market sold data for ZIP ${zip}`);
  const items = await runApifyActor('maxcopell/zillow-scraper', {
    startUrls: [{ url: soldUrl }],
    maxItems: 40,
  });

  if (!items || items.length === 0) return null;

  const domValues = items
    .map(item => item.daysOnMarket ?? item.timeOnZillow ?? item.days_on_zillow ?? null)
    .filter(v => v !== null && !isNaN(Number(v)))
    .map(Number);

  if (domValues.length === 0) return null;

  const avg = domValues.reduce((a, b) => a + b, 0) / domValues.length;
  return Math.round(avg);
}

// ─── Sheet read/write ─────────────────────────────────────────────────────────

async function readAddresses(sheets) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `'${SHEET_TAB}'!A:A`,
  });

  const rows = res.data.values || [];
  // Skip header rows (first 2), collect non-empty addresses with their 1-based row index
  const addresses = [];
  for (let i = 2; i < rows.length; i++) {
    const addr = (rows[i][0] || '').trim();
    if (addr) addresses.push({ address: addr, rowIndex: i + 1 }); // rowIndex is 1-based
  }
  return addresses;
}

async function updateSheetRow(sheets, rowIndex, data) {
  // Build a sparse row update using individual range writes to avoid
  // overwriting columns we don't own.
  const colLetter = n => String.fromCharCode(65 + n);

  const updates = [
    { col: COL.price,        value: data.price        ?? '' },
    { col: COL.zillowUrl,    value: data.zillowUrl    ?? '' },
    { col: COL.listDate,     value: data.listDate     ?? '' },
    { col: COL.soldPrice,    value: data.soldPrice    ?? '' },
    { col: COL.soldDate,     value: data.soldDate     ?? '' },
    { col: COL.daysOnMarket, value: data.daysOnMarket ?? '' },
    { col: COL.marketAvgDom, value: data.marketAvgDom ?? '' },
  ].map(({ col, value }) => ({
    range: `'${SHEET_TAB}'!${colLetter(col)}${rowIndex}`,
    values: [[value]],
  }));

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      valueInputOption: 'USER_ENTERED',
      data: updates,
    },
  });
}

// ─── Zapier notification ──────────────────────────────────────────────────────

async function sendToZapier(payload) {
  if (!ZAPIER_WEBHOOK) {
    console.warn('  ZAPIER_WEBHOOK not set, skipping notification');
    return;
  }
  const res = await fetch(ZAPIER_WEBHOOK, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    console.warn(`  Zapier webhook returned ${res.status}`);
  }
}

// ─── Main job ─────────────────────────────────────────────────────────────────

async function runMonitor() {
  console.log(`[${new Date().toISOString()}] Starting property monitor run`);

  const sheets = getSheetsClient();

  let addressRows;
  try {
    addressRows = await readAddresses(sheets);
  } catch (err) {
    console.error('Failed to read addresses from sheet:', err);
    return;
  }

  console.log(`Found ${addressRows.length} addresses to process`);

  // Cache market DOM per ZIP so we only call Apify once per ZIP
  const domCache = {};

  for (const { address, rowIndex } of addressRows) {
    console.log(`\nProcessing [row ${rowIndex}]: ${address}`);

    let detail = null;
    try {
      detail = await fetchListingDetail(address);
    } catch (err) {
      console.error(`  Error fetching detail: ${err.message}`);
    }

    const zip = extractZip(address);
    if (zip && !(zip in domCache)) {
      try {
        domCache[zip] = await fetchMarketAvgDom(zip);
      } catch (err) {
        console.error(`  Error fetching market DOM for ZIP ${zip}: ${err.message}`);
        domCache[zip] = null;
      }
    }

    const marketAvgDom = zip ? domCache[zip] : null;

    // Map Apify result fields (field names vary by scraper version)
    const price        = detail?.price ?? detail?.listPrice ?? detail?.unformattedPrice ?? '';
    const zillowUrl    = detail?.url   ?? detail?.hdpUrl    ?? detail?.detailUrl         ?? '';
    const listDate     = detail?.listingUpdatedDate ?? detail?.datePosted ?? detail?.dateListed ?? '';
    const soldPrice    = detail?.soldPrice ?? detail?.lastSoldPrice ?? '';
    const soldDate     = detail?.soldDate  ?? detail?.lastSoldDate  ?? '';
    const daysOnMarket = detail?.daysOnMarket ?? detail?.timeOnZillow ?? detail?.days_on_zillow ?? '';

    const rowData = { price, zillowUrl, listDate, soldPrice, soldDate, daysOnMarket, marketAvgDom };

    try {
      await updateSheetRow(sheets, rowIndex, rowData);
      console.log(`  Sheet updated (row ${rowIndex})`);
    } catch (err) {
      console.error(`  Failed to update sheet: ${err.message}`);
    }

    try {
      await sendToZapier({ address, rowIndex, ...rowData, updatedAt: new Date().toISOString() });
      console.log('  Zapier notified');
    } catch (err) {
      console.error(`  Failed to notify Zapier: ${err.message}`);
    }
  }

  console.log(`\n[${new Date().toISOString()}] Monitor run complete`);
}

// ─── Scheduler ────────────────────────────────────────────────────────────────

// Every day at 08:00 America/Phoenix (UTC-7, no DST)
cron.schedule('0 8 * * *', () => {
  runMonitor().catch(err => console.error('Unhandled error in monitor run:', err));
}, {
  timezone: 'America/Phoenix',
});

console.log('Property monitor started. Scheduled daily at 08:00 America/Phoenix.');

// Allow an immediate run via env flag (useful for testing on Railway)
if (process.env.RUN_NOW === 'true') {
  runMonitor().catch(err => console.error('Unhandled error in immediate run:', err));
}

'use strict';

/**
 * aryeo-import.js — one-time backfill script
 *
 * Reads Orders xlsx export from Aryeo and writes each row to the
 * "Listing Archive" tab of the Google Sheet.
 *
 * Data sources (xlsx sheets):
 *   Orders      → Address (G/6), Agent (C/2), Shoot Date (M/12 or N/13)
 *   Order Items → Item name (col 5) joined by Order ID for package
 *
 * Written to sheet columns: A=Address, B=Agent, C=Shoot Date, D=Package
 * Clears rows 3+ before writing, then writes all unique addresses from xlsx.
 *
 * Usage:
 *   node --env-file=.env aryeo-import.js
 */

const XLSX       = require('xlsx');
const { google } = require('googleapis');

const XLSX_PATH   = '/Users/loganharding/Downloads/Orders - Mar 31 2026.xlsx';
const SHEET_ID    = process.env.GOOGLE_SHEET_ID;
const ARCHIVE_TAB = 'Listing Archive';

// ─── Google Sheets auth ───────────────────────────────────────────────────────

function getSheetsClient() {
  const auth = new google.auth.JWT({
    email:  process.env.GOOGLE_CLIENT_EMAIL,
    key:    (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// ─── Date parsing ─────────────────────────────────────────────────────────────

// Parses Aryeo date strings like "Tue, Mar 31 2026, 1:40pm MST" → "2026-03-31"
function parseAryeoDate(raw) {
  if (!raw) return '';
  const s = String(raw).trim();
  // Strip time and timezone ("1:40pm MST") leaving just the date part
  const dateOnly = s.replace(/,?\s+\d{1,2}:\d{2}[ap]m\s*\w*/i, '').trim();
  const d = new Date(dateOnly);
  if (!isNaN(d.getTime())) {
    return d.toISOString().slice(0, 10);
  }
  return s; // fallback: return as-is
}

// ─── Read xlsx ────────────────────────────────────────────────────────────────

function readOrders() {
  const wb = XLSX.readFile(XLSX_PATH);

  // Build a map of Order ID → comma-joined package names from Order Items sheet.
  const itemsSheet = wb.Sheets['Order Items'];
  const itemRows   = XLSX.utils.sheet_to_json(itemsSheet, { header: 1, defval: '' });
  const packageMap = {}; // orderId → Set of item names
  for (let i = 1; i < itemRows.length; i++) {
    const r       = itemRows[i];
    const orderId = String(r[0] || '').trim();
    const item    = String(r[5] || '').trim();
    if (!orderId || !item) continue;
    if (!packageMap[orderId]) packageMap[orderId] = new Set();
    packageMap[orderId].add(item);
  }

  // Read Orders sheet.
  const ordersSheet = wb.Sheets['Orders'];
  const rows        = XLSX.utils.sheet_to_json(ordersSheet, { header: 1, defval: '' });

  const seen   = new Set();
  const orders = [];
  for (let i = 1; i < rows.length; i++) {
    const row     = rows[i];
    const address = String(row[6] || '').trim();
    if (!address || address.toLowerCase() === 'no address') continue;
    if (seen.has(address.toLowerCase())) continue; // keep first occurrence per address
    seen.add(address.toLowerCase());

    const orderId   = String(row[0] || '').trim();
    const agent     = String(row[2] || '').trim();
    const shootDate = parseAryeoDate(row[12] || row[13]); // First Fulfilled At, then Created At
    const pkgSet    = packageMap[orderId];
    const pkg       = pkgSet ? [...pkgSet].join(', ') : '';

    orders.push([address, agent, shootDate, pkg]);
  }
  return orders;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const sheets = getSheetsClient();

  // Get sheetId for resize operation.
  console.log('Fetching sheet metadata...');
  const meta      = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheetMeta = meta.data.sheets.find(s => s.properties.title === ARCHIVE_TAB);
  if (!sheetMeta) throw new Error(`Tab "${ARCHIVE_TAB}" not found`);
  const sheetId     = sheetMeta.properties.sheetId;
  const currentRows = sheetMeta.properties.gridProperties.rowCount;

  // Ensure the sheet has enough rows, then clear data rows (3+).
  const neededRows = Math.max(currentRows, 1000);
  console.log(`Resizing sheet to ${neededRows} rows and clearing data rows (row 3+)...`);
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [{
        updateSheetProperties: {
          properties: { sheetId, gridProperties: { rowCount: neededRows } },
          fields: 'gridProperties.rowCount',
        },
      }],
    },
  });
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SHEET_ID,
    range: `'${ARCHIVE_TAB}'!A3:Z${neededRows}`,
  });

  // Read and deduplicate orders from xlsx.
  const rows = readOrders();
  console.log(`Read ${rows.length} unique address(es) from xlsx.`);

  if (rows.length === 0) {
    console.log('Nothing to write.');
    return;
  }

  // Write all rows starting at row 3.
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `'${ARCHIVE_TAB}'!A3`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: rows },
  });

  console.log(`Done. Wrote ${rows.length} row(s) to rows 3–${2 + rows.length}.`);
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});

'use strict';

/**
 * aryeo-import.js — one-time backfill script
 *
 * Reads Orders xlsx export from Aryeo and writes each row to the
 * "Listing Archive" tab of the Google Sheet.
 *
 * Column mapping from xlsx (0-based):
 *   G (6)  = Address
 *   C (2)  = Agent/Client (Customer)
 *   M (12) = Shoot Date (First Fulfilled At), falls back to N (13) Created At
 *   W (22) = Package (Order Form Name)
 *
 * Written to sheet columns: A=Address, B=Agent, C=Shoot Date, D=Package
 * Starts at row 3 (row 1 = legend, row 2 = headers).
 * Skips rows where Address already exists in column A.
 *
 * Usage:
 *   node --env-file=.env aryeo-import.js
 */

const XLSX        = require('xlsx');
const { google }  = require('googleapis');
const path        = require('path');

const XLSX_PATH  = '/Users/loganharding/Downloads/Orders - Mar 31 2026.xlsx';
const SHEET_ID   = process.env.GOOGLE_SHEET_ID;
const ARCHIVE_TAB = 'Listing Archive';

// ─── Google Sheets auth ───────────────────────────────────────────────────────

function getSheetsClient() {
  const auth = new google.auth.JWT({
    email: process.env.GOOGLE_CLIENT_EMAIL,
    key:   (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n'),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

// ─── Read xlsx ────────────────────────────────────────────────────────────────

function readOrders() {
  const wb   = XLSX.readFile(XLSX_PATH);
  const ws   = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });

  // Skip header row (index 0)
  const orders = [];
  for (let i = 1; i < rows.length; i++) {
    const row     = rows[i];
    const address = String(row[6] || '').trim();
    if (!address) continue;

    const agent     = String(row[2]  || '').trim();
    const shootDate = String(row[12] || row[13] || '').trim();
    const pkg       = String(row[22] || '').trim();

    orders.push({ address, agent, shootDate, pkg });
  }
  return orders;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const sheets = getSheetsClient();

  // Read existing addresses from column A to detect duplicates.
  console.log('Reading existing addresses from sheet...');
  const existing = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `'${ARCHIVE_TAB}'!A:A`,
  });
  const existingAddresses = new Set(
    (existing.data.values || []).flat().map(v => String(v).trim().toLowerCase())
  );
  console.log(`Found ${existingAddresses.size} existing address(es) in sheet.`);

  // Read orders from xlsx.
  const orders = readOrders();
  console.log(`Read ${orders.length} order(s) from xlsx.`);

  // Determine the next empty row (at least row 3).
  const nextRow = Math.max(3, existingAddresses.size + 1);

  // Filter to new addresses only and build sheet rows.
  const newRows = [];
  let skipped = 0;
  for (const { address, agent, shootDate, pkg } of orders) {
    if (existingAddresses.has(address.toLowerCase())) {
      console.log(`  Skipping duplicate: ${address}`);
      skipped++;
      continue;
    }
    newRows.push([address, agent, shootDate, pkg]);
    existingAddresses.add(address.toLowerCase()); // prevent duplicates within this batch
  }

  console.log(`Writing ${newRows.length} new row(s) (skipped ${skipped} duplicate(s))...`);

  if (newRows.length === 0) {
    console.log('Nothing to write.');
    return;
  }

  // Write all new rows in one batchUpdate starting at nextRow.
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `'${ARCHIVE_TAB}'!A${nextRow}`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: newRows },
  });

  console.log(`Done. Wrote rows ${nextRow}–${nextRow + newRows.length - 1}.`);
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});

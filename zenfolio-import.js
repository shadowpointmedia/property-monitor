'use strict';

/**
 * zenfolio-import.js — one-time backfill script
 *
 * Walks the Zenfolio folder tree for shadowpointmedia and finds all
 * address-level folders 3 levels deep:
 *   Root > Realtors > [Realtor Name] > [Address]
 *
 * Prints extracted records as JSON to stdout. Does NOT write to Sheets.
 *
 * Usage:
 *   node --env-file=.env zenfolio-import.js
 */

const fetch  = require('node-fetch');

const ZENFOLIO_API   = 'https://api.zenfolio.com/api/1.8/zfapi.asmx';
const ZENFOLIO_LOGIN = 'shadowpointmedia';

// ─── JSON-RPC helper ──────────────────────────────────────────────────────────

let _reqId = 1;

async function callApi(method, params, token) {
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers['X-Zenfolio-Token'] = token;

  const res = await fetch(ZENFOLIO_API, {
    method:  'POST',
    headers,
    body:    JSON.stringify({ method, params, id: _reqId++ }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status} calling ${method}: ${text.slice(0, 200)}`);
  }

  const json = await res.json();
  if (json.error) throw new Error(`Zenfolio API error in ${method}: ${JSON.stringify(json.error)}`);
  return json.result;
}

// ─── Folder tree walk ─────────────────────────────────────────────────────────

// Returns the creation date for a node (Group or PhotoSet).
// Falls back to null if no date is available.
function nodeDate(node) {
  // PhotoSet has CreatedOn; Group may have CreatedOn too
  const raw = node.CreatedOn || null;
  if (!raw) return null;
  // Zenfolio returns dates as "/Date(1234567890123)/" or ISO strings
  if (typeof raw === 'string') {
    const ms = raw.match(/\/Date\((\d+)\)\//);
    if (ms) return new Date(parseInt(ms[1])).toISOString().slice(0, 10);
    return raw.slice(0, 10); // ISO fallback
  }
  return null;
}

// Recursively finds children of a group node.
// Zenfolio tree nodes have an Elements array containing Groups and PhotoSets.
function children(node) {
  return node.Elements || [];
}

function isGroup(node) {
  return node.$type === 'Group' || node.GroupIndex !== undefined;
}

function extractAddresses(root) {
  const results = [];

  // Find the "Realtors" top-level group
  const realtorsGroup = children(root).find(
    n => isGroup(n) && (n.Title || '').trim().toLowerCase() === 'realtors'
  );

  if (!realtorsGroup) {
    console.error('Warning: No "Realtors" group found at the top level.');
    return results;
  }

  // Walk: Realtors > [Realtor] > [Address]
  for (const realtorNode of children(realtorsGroup)) {
    if (!isGroup(realtorNode)) continue;
    const realtorName = (realtorNode.Title || '').trim();

    for (const addressNode of children(realtorNode)) {
      const address   = (addressNode.Title || '').trim();
      const shootDate = nodeDate(addressNode);

      results.push({ address, shootDate, realtorName });
    }
  }

  return results;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  // LoadGroupHierarchy is publicly accessible for public accounts — no auth needed.
  console.error('Loading group hierarchy...');
  const root = await callApi('LoadGroupHierarchy', [ZENFOLIO_LOGIN]);
  console.error('Group hierarchy loaded.');

  const records = extractAddresses(root);

  console.error(`\nFound ${records.length} address folder(s).\n`);
  console.log(JSON.stringify(records, null, 2));
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});

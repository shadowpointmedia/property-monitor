'use strict';

/**
 * zenfolio-import.js — one-time backfill script
 *
 * Walks the Zenfolio folder tree for shadowpointmedia and finds all
 * address-level folders 3 levels deep:
 *   Root > REALTORS > [Realtor Name] > [Address]
 *
 * Prints extracted records as JSON to stdout. Does NOT write to Sheets.
 *
 * Usage:
 *   node --env-file=.env zenfolio-import.js
 */

const fetch  = require('node-fetch');
const crypto = require('crypto');

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

// ─── Authentication ────────────────────────────────────────────────────────────

async function authenticate() {
  const password = process.env.ZENFOLIO_PASSWORD;
  if (!password) throw new Error('ZENFOLIO_PASSWORD env var is not set');

  console.error('Authenticating with Zenfolio...');
  const challenge = await callApi('GetChallenge', [ZENFOLIO_LOGIN]);

  const saltBytes      = Buffer.from(challenge.PasswordSalt);
  const challengeBytes = Buffer.from(challenge.Challenge);
  const passwordBytes  = Buffer.from(password, 'utf8');

  const innerHash = crypto.createHash('sha256')
    .update(Buffer.concat([saltBytes, passwordBytes]))
    .digest();

  const passwordHash = Array.from(
    crypto.createHash('sha256')
      .update(Buffer.concat([challengeBytes, innerHash]))
      .digest()
  );

  const token = await callApi('Authenticate', [ZENFOLIO_LOGIN, passwordHash]);
  if (!token) throw new Error('Authenticate returned empty token');

  console.error('Authenticated. Token received.');
  return token;
}

// ─── Date parsing ─────────────────────────────────────────────────────────────

function nodeDate(node) {
  const raw = node.CreatedOn || null;
  if (!raw) return null;
  // Zenfolio wraps dates: { "$type": "DateTime", "Value": "2024-07-19 00:24:06" }
  if (typeof raw === 'object' && raw.Value) return raw.Value.slice(0, 10);
  if (typeof raw === 'string') {
    const ms = raw.match(/\/Date\((\d+)\)\//);
    if (ms) return new Date(parseInt(ms[1])).toISOString().slice(0, 10);
    return raw.slice(0, 10);
  }
  return null;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const token = await authenticate();

  // Load the top-level group hierarchy with auth so password-protected groups are visible.
  console.error('Loading group hierarchy...');
  const root = await callApi('LoadGroupHierarchy', [ZENFOLIO_LOGIN], token);
  console.error('Group hierarchy loaded.');

  // Find the REALTORS group (case-insensitive).
  const realtorsGroup = (root.Elements || []).find(
    n => n.$type === 'Group' && (n.Title || '').trim().toUpperCase() === 'REALTORS'
  );
  if (!realtorsGroup) throw new Error('No REALTORS group found in account root.');

  const realtorNodes = realtorsGroup.Elements || [];
  console.error(`Found ${realtorNodes.length} realtor group(s). Loading each...`);

  const records = [];

  for (const realtorNode of realtorNodes) {
    if (realtorNode.$type !== 'Group') continue;
    const realtorName = (realtorNode.Title || '').trim();

    // LoadGroupHierarchy only returns 2 levels deep; load each realtor group
    // individually with the auth token to get its address subfolders.
    let loaded;
    try {
      loaded = await callApi('LoadGroup', [realtorNode.Id, 'Level1'], token);
    } catch (err) {
      console.error(`  Skipping "${realtorName}": ${err.message}`);
      continue;
    }

    const addressNodes = (loaded && loaded.Elements) ? loaded.Elements : [];
    if (addressNodes.length === 0) {
      console.error(`  "${realtorName}": 0 address folders`);
      continue;
    }

    console.error(`  "${realtorName}": ${addressNodes.length} address folder(s)`);
    for (const addressNode of addressNodes) {
      const address   = (addressNode.Title || '').trim();
      const shootDate = nodeDate(addressNode);
      records.push({ address, shootDate, realtorName });
    }
  }

  console.error(`\nTotal: ${records.length} address folder(s) found.\n`);
  console.log(JSON.stringify(records, null, 2));
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});

'use strict';

/**
 * zillow-historical.js — one-time backfill script
 *
 * For each row in "Listing Archive" that lacks a list price (col E) and ZPID (col P):
 *   1. Extracts ZIP from address, uses ZIP_SOLD_URLS to search Zillow via zillow-scraper
 *   2. Matches the property by street address to get ZPID
 *   3. Fetches full price history via zillow-detail-scraper
 *   4. Finds the listing cycle that started within 90 days after the shoot date
 *   5. Writes: E=listPrice, F=zillowUrl, G=listDate, H=soldPrice, I=soldDate,
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

// ZIP → Zillow sold search URL (same as index.js)
const ZIP_SOLD_URLS = {
  '85718': 'https://www.zillow.com/tucson-az-85718/sold/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-111.14097846728515%2C%22east%22%3A-110.69534553271484%2C%22south%22%3A32.20951945556616%2C%22north%22%3A32.4717400226085%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95056%2C%22regionType%22%3A7%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%2C%22usersSearchTerm%22%3A%2285718%22%7D',
  '85755': 'https://www.zillow.com/oro-valley-az-85755/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.55265469222126%2C%22south%22%3A32.421757054341654%2C%22east%22%3A-110.87567826635743%2C%22west%22%3A-111.09849473364258%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%2C%22usersSearchTerm%22%3A%22Oro%20Valley%20AZ%2085755%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95090%2C%22regionType%22%3A7%7D%5D%2C%22pagination%22%3A%7B%7D%7D',
  '85742': 'https://www.zillow.com/oro-valley-az-85742/sold/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-111.14097846728515%2C%22east%22%3A-110.69534553271484%2C%22south%22%3A32.20951945556616%2C%22north%22%3A32.4717400226085%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95056%2C%22regionType%22%3A7%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%2C%22usersSearchTerm%22%3A%2285742%22%7D',
  '85710': 'https://www.zillow.com/tucson-az-85710/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.24822205296588%2C%22south%22%3A32.18257624907564%2C%22east%22%3A-110.76816588317871%2C%22west%22%3A-110.87957411682129%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A14%2C%22usersSearchTerm%22%3A%2285710%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95048%2C%22regionType%22%3A7%7D%5D%7D',
  '85749': 'https://www.zillow.com/tucson-az-85749/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.353240094287756%2C%22south%22%3A32.22205294579666%2C%22east%22%3A-110.60281176635742%2C%22west%22%3A-110.82562823364258%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%2C%22usersSearchTerm%22%3A%2285749%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95085%2C%22regionType%22%3A7%7D%5D%7D',
  '85750': 'https://www.zillow.com/tucson-az-85750/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.36442614768735%2C%22south%22%3A32.23325519700705%2C%22east%22%3A-110.71940326635743%2C%22west%22%3A-110.94221973364259%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%2C%22usersSearchTerm%22%3A%2285750%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95086%2C%22regionType%22%3A7%7D%5D%7D',
  '85719': 'https://www.zillow.com/tucson-az-85719/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.31310860934659%2C%22south%22%3A32.18186339025408%2C%22east%22%3A-110.83788976635742%2C%22west%22%3A-111.06070623364258%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%2C%22usersSearchTerm%22%3A%2285719%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95057%2C%22regionType%22%3A7%7D%5D%7D',
  '85658': 'https://www.zillow.com/marana-az-85658/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.86484841848326%2C%22south%22%3A32.34193812209974%2C%22east%22%3A-110.73438756542969%2C%22west%22%3A-111.62565343457031%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A11%2C%22usersSearchTerm%22%3A%2285658%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A399662%2C%22regionType%22%3A7%7D%5D%7D',
  '85641': 'https://www.zillow.com/vail-az-85641/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.56603190899519%2C%22south%22%3A31.513702683952083%2C%22east%22%3A-109.79180113085937%2C%22west%22%3A-111.57433286914062%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285641%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95025%2C%22regionType%22%3A7%7D%5D%7D',
  '85711': 'https://www.zillow.com/tucson-az-85711/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.2459792108915%2C%22south%22%3A32.1803317872733%2C%22east%22%3A-110.83149038317872%2C%22west%22%3A-110.9428986168213%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285711%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95049%2C%22regionType%22%3A7%7D%5D%2C%22mapZoom%22%3A14%7D',
  '85704': 'https://www.zillow.com/tucson-az-85704/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.40404605140429%2C%22south%22%3A32.272932512079194%2C%22east%22%3A-110.87131926635743%2C%22west%22%3A-111.09413573364259%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285704%22%2C%22mapZoom%22%3A13%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95042%2C%22regionType%22%3A7%7D%5D%7D',
  '85716': 'https://www.zillow.com/tucson-az-85716/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.31048769268923%2C%22south%22%3A32.179238683352374%2C%22east%22%3A-110.8122232663574%2C%22west%22%3A-111.03503973364256%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285716%22%2C%22mapZoom%22%3A13%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95054%2C%22regionType%22%3A7%7D%5D%7D',
  '85745': 'https://www.zillow.com/tucson-az-85745/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.401648120934915%2C%22south%22%3A32.13922434653764%2C%22east%22%3A-110.87548153271484%2C%22west%22%3A-111.32111446728516%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285745%22%2C%22mapZoom%22%3A12%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95081%2C%22regionType%22%3A7%7D%5D%7D',
  '85737': 'https://www.zillow.com/oro-valley-az-85737/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.48495920829817%2C%22south%22%3A32.3539631116095%2C%22east%22%3A-110.84131376635742%2C%22west%22%3A-111.06413023364257%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285737%22%2C%22mapZoom%22%3A13%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95073%2C%22regionType%22%3A7%7D%5D%7D',
  '85701': 'https://www.zillow.com/tucson-az-85701/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.23499466638402%2C%22south%22%3A32.202172911629994%2C%22east%22%3A-110.94146294158936%2C%22west%22%3A-110.99716705841065%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285701%22%2C%22mapZoom%22%3A15%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95039%2C%22regionType%22%3A7%7D%5D%7D',
  '85715': 'https://www.zillow.com/tucson-az-85715/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.28589757980452%2C%22south%22%3A32.22027899934747%2C%22east%22%3A-110.77244888317871%2C%22west%22%3A-110.88385711682129%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285715%22%2C%22mapZoom%22%3A14%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95053%2C%22regionType%22%3A7%7D%5D%7D',
  '85705': 'https://www.zillow.com/tucson-az-85705/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.335123641556024%2C%22south%22%3A32.20391027041503%2C%22east%22%3A-110.89349076635742%2C%22west%22%3A-111.11630723364257%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285705%22%2C%22mapZoom%22%3A13%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95043%2C%22regionType%22%3A7%7D%5D%7D',
  '85739': 'https://www.zillow.com/saddlebrooke-az-85739/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.63299298618958%2C%22south%22%3A32.37124141254122%2C%22east%22%3A-110.65231303271483%2C%22west%22%3A-111.09794596728514%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285739%22%2C%22mapZoom%22%3A12%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95075%2C%22regionType%22%3A7%7D%5D%7D',
  '85741': 'https://www.zillow.com/tucson-az-85741/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.40159685846508%2C%22south%22%3A32.27047976830646%2C%22east%22%3A-110.93519476635741%2C%22west%22%3A-111.15801123364257%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285741%22%2C%22mapZoom%22%3A13%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95077%2C%22regionType%22%3A7%7D%5D%7D',
  '85648': 'https://www.zillow.com/rio-rico-az-85648/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A31.824223761082752%2C%22south%22%3A31.295308425950502%2C%22east%22%3A-110.57029256542968%2C%22west%22%3A-111.4615584345703%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285648%22%2C%22mapZoom%22%3A11%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95030%2C%22regionType%22%3A7%7D%5D%7D',
  '85646': 'https://www.zillow.com/tubac-az-85646/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A31.75539653965929%2C%22south%22%3A31.491117803785038%2C%22east%22%3A-110.86400553271484%2C%22west%22%3A-111.30963846728515%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285646%22%2C%22mapZoom%22%3A12%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95029%2C%22regionType%22%3A7%7D%5D%7D',
  '85730': 'https://www.zillow.com/tucson-az-85730/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.30851597429146%2C%22south%22%3A32.04582280556141%2C%22east%22%3A-110.50552353271486%2C%22west%22%3A-110.95115646728517%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285730%22%2C%22mapZoom%22%3A12%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95066%2C%22regionType%22%3A7%7D%5D%7D',
  '85619': 'https://www.zillow.com/mount-lemmon-az-85619/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.55637382488952%2C%22south%22%3A32.294399149687074%2C%22east%22%3A-110.56752303271485%2C%22west%22%3A-111.01315596728516%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285619%22%2C%22mapZoom%22%3A12%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95003%2C%22regionType%22%3A7%7D%5D%7D',
  '85622': 'https://www.zillow.com/green-valley-az-85622/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A31.88034175281661%2C%22south%22%3A31.74847442213559%2C%22east%22%3A-110.94434176635741%2C%22west%22%3A-111.16715823364257%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285622%22%2C%22mapZoom%22%3A13%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95006%2C%22regionType%22%3A7%7D%5D%7D',
  '85635': 'https://www.zillow.com/sierra-vista-az-85635/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A31.707359788190182%2C%22south%22%3A31.44294451584624%2C%22east%22%3A-110.96920103271485%2C%22west%22%3A-110.41483396728516%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285635%22%2C%22mapZoom%22%3A12%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95019%2C%22regionType%22%3A7%7D%5D%7D',
  '85653': 'https://www.zillow.com/marana-az-85653/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.621629324500454%2C%22south%22%3A32.0972998506856%2C%22east%22%3A-110.91574206542971%2C%22west%22%3A-111.80700793457034%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285653%22%2C%22mapZoom%22%3A11%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95033%2C%22regionType%22%3A7%7D%5D%7D',
  '85743': 'https://www.zillow.com/tucson-az-85743/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.42790810272516%2C%22south%22%3A32.165560413921014%2C%22east%22%3A-110.93997053271484%2C%22west%22%3A-111.38560346728515%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285743%22%2C%22mapZoom%22%3A12%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95079%2C%22regionType%22%3A7%7D%5D%7D',
  '85747': 'https://www.zillow.com/tucson-az-85747/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.221759997810835%2C%22south%22%3A31.95881650559023%2C%22east%22%3A-110.53549303271485%2C%22west%22%3A-110.98112596728517%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285747%22%2C%22mapZoom%22%3A12%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95083%2C%22regionType%22%3A7%7D%5D%7D',
  '85748': 'https://www.zillow.com/tucson-az-85748/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.28350518993753%2C%22south%22%3A32.1522171757991%2C%22east%22%3A-110.61838426635742%2C%22west%22%3A-110.84120073364258%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285748%22%2C%22mapZoom%22%3A13%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95084%2C%22regionType%22%3A7%7D%5D%7D',
  '85712': 'https://www.zillow.com/tucson-az-85712/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.288060837240906%2C%22south%22%3A32.22244382076419%2C%22east%22%3A-110.8261388831787%2C%22west%22%3A-110.93754711682128%7D%2C%22mapZoom%22%3A14%2C%22usersSearchTerm%22%3A%2285712%22%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95050%2C%22regionType%22%3A7%7D%5D%7D',
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

async function runApifyActor(actorId, input, fetchLimit = 150) {
  const safeId   = actorId.replace('/', '~');
  const startRes = await fetch(
    `${APIFY_BASE}/acts/${safeId}/runs?token=${APIFY_TOKEN}&memory=2048`,
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

  const itemsRes = await fetch(`${APIFY_BASE}/actor-runs/${runId}/dataset/items?token=${APIFY_TOKEN}&limit=${fetchLimit}`);
  if (!itemsRes.ok) throw new Error(`Failed to fetch dataset for run ${runId}`);
  return itemsRes.json();
}

// ─── Price history analysis ───────────────────────────────────────────────────

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

function findListingCycle(priceHistory, shootDate) {
  if (!Array.isArray(priceHistory) || !shootDate) return null;

  const shoot   = new Date(shootDate);
  const earliest = shoot;
  const latest   = new Date(shoot.getTime() + 90 * 24 * 60 * 60 * 1000);

  const sorted = [...priceHistory].sort((a, b) => {
    const da = parseHistoryDate(a.time || a.date);
    const db = parseHistoryDate(b.time || b.date);
    return (da?.getTime() ?? 0) - (db?.getTime() ?? 0);
  });

  const listEvent = sorted.find(e => {
    if (!/listed\s+for\s+sale/i.test(e.event || '')) return false;
    const d = parseHistoryDate(e.time || e.date);
    return d && d >= earliest && d <= latest;
  });

  if (!listEvent) return null;

  const listDate  = parseHistoryDate(listEvent.time || listEvent.date);
  const listPrice = listEvent.price ?? null;

  const soldEvent = sorted.find(e => {
    if (!/sold/i.test(e.event || '')) return false;
    const d = parseHistoryDate(e.time || e.date);
    return d && listDate && d > listDate;
  });

  const soldDate  = soldEvent ? parseHistoryDate(soldEvent.time || soldEvent.date) : null;
  const soldPrice = soldEvent?.price ?? null;

  const endDate = soldDate || new Date();
  const dom     = listDate ? Math.round((endDate - listDate) / (1000 * 60 * 60 * 24)) : null;

  return {
    listPrice,
    listDate:     toYMD(listDate),
    soldPrice,
    soldDate:     toYMD(soldDate),
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

// Build an active listings URL from a sold URL by removing the rs:true filter.
function buildActiveUrl(soldUrl) {
  try {
    const u   = new URL(soldUrl);
    const qs  = u.searchParams.get('searchQueryState');
    if (!qs) return null;
    const state = JSON.parse(decodeURIComponent(qs));
    delete state.filterState?.rs;
    // Remove /sold/ path segment
    u.pathname = u.pathname.replace('/sold/', '/');
    u.searchParams.set('searchQueryState', JSON.stringify(state));
    return u.toString();
  } catch {
    return null;
  }
}

function extractZip(address) {
  // Match 5-digit ZIP at the end of the address (after state abbreviation)
  return (address.match(/\b([A-Z]{2})\s+(\d{5})\b/) || [])[2] || null;
}

function matchByStreet(items, address) {
  const street = address.split(',')[0].trim().toLowerCase();
  return items.find(i => {
    const s = (i.addressStreet || i.address || '').toLowerCase();
    return s.includes(street) || street.includes(s.split(',')[0].trim());
  }) || null;
}

// Step 1: search by ZIP sold URL to find the ZPID
async function findZpidByAddress(address) {
  const zip = extractZip(address);
  if (!zip || !ZIP_SOLD_URLS[zip]) {
    console.warn(`  No ZIP_SOLD_URLS entry for ZIP ${zip || '(none)'}`);
    return null;
  }

  const soldUrl = ZIP_SOLD_URLS[zip];

  // Try sold listings first
  let items = await runApifyActor('maxcopell/zillow-scraper', {
    searchUrls: [{ url: soldUrl }],
    maxItems:   150,
  }, 150);

  if (items?.length) {
    const match = matchByStreet(items, address);
    if (match?.zpid) {
      console.log(`  Found ZPID ${match.zpid} in sold listings`);
      return String(match.zpid);
    }
  }

  // Fallback: try active listings URL
  const activeUrl = buildActiveUrl(soldUrl);
  if (activeUrl) {
    console.log(`  Not found in sold listings, trying active listings...`);
    items = await runApifyActor('maxcopell/zillow-scraper', {
      searchUrls: [{ url: activeUrl }],
      maxItems:   150,
    }, 150);
    if (items?.length) {
      const match = matchByStreet(items, address);
      if (match?.zpid) {
        console.log(`  Found ZPID ${match.zpid} in active listings`);
        return String(match.zpid);
      }
    }
  }

  return null;
}

// Step 2: fetch full price history using ZPID URL
async function fetchDetailByZpid(zpid) {
  const url   = `https://www.zillow.com/homedetails/home/${zpid}_zpid/`;
  const items = await runApifyActor('maxcopell/zillow-detail-scraper', {
    startUrls: [{ url }],
    maxItems:  1,
  }, 1);
  return items?.[0] ?? null;
}

async function lookupZillow(address) {
  const zpid = await findZpidByAddress(address);
  if (!zpid) return null;
  const detail = await fetchDetailByZpid(zpid);
  if (detail) detail._zpid = zpid;
  return detail;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const sheets = getSheetsClient();

  console.log('Reading Listing Archive...');
  const rows = await readArchiveRows(sheets);
  console.log(`Found ${rows.length} row(s) total.`);

  const toProcess = rows.filter(r => !r.listPrice);
  console.log(`${toProcess.length} row(s) need processing (no list price).\n`);

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

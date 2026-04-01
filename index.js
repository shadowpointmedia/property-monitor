'use strict';

const cron = require('node-cron');
const fetch = require('node-fetch');
const { google } = require('googleapis');

// ─── Config ──────────────────────────────────────────────────────────────────

const SHEET_ID            = process.env.GOOGLE_SHEET_ID;
const SHEET_TAB           = 'Listing Tracker';
const ARCHIVE_TAB         = 'Listing Archive';
const GRADUATE_AFTER_DAYS = 90;
const APIFY_TOKEN         = process.env.APIFY_TOKEN;

// 0-based column indices  →  sheet letter
// 0-based column indices matching "Listing Tracker" row 2 headers
const COL = {
  address:      0,  // A  – Address
  listPrice:    4,  // E  – List Price ($)
  zillowUrl:    5,  // F  – Zillow / MLS URL
  listDate:     6,  // G  – List Date
  soldPrice:    7,  // H  – Sold Price ($)
  // I (index 8) is a duplicate "List Date" header — skipped
  soldDate:     9,  // J  – Sold Date
  daysOnMarket: 10, // K  – Days on Market
  marketAvgDom: 11, // L  – Market Avg DOM
  // M/N/O (12-14) are formula columns — not written by this script
  zpid:         15, // P  – ZPID cache (persisted; empty on first run)
};

// ZIP → Zillow sold-homes search URL (must include full searchQueryState)
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
  '85635': 'https://www.zillow.com/sierra-vista-az-85635/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A31.707359788190182%2C%22south%22%3A31.44294451584624%2C%22east%22%3A-109.96920103271485%2C%22west%22%3A-110.41483396728516%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285635%22%2C%22mapZoom%22%3A12%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95019%2C%22regionType%22%3A7%7D%5D%7D',
  '85653': 'https://www.zillow.com/marana-az-85653/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.621629324500454%2C%22south%22%3A32.0972998506856%2C%22east%22%3A-110.91574206542971%2C%22west%22%3A-111.80700793457034%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285653%22%2C%22mapZoom%22%3A11%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95033%2C%22regionType%22%3A7%7D%5D%7D',
  '85743': 'https://www.zillow.com/tucson-az-85743/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.42790810272516%2C%22south%22%3A32.165560413921014%2C%22east%22%3A-110.93997053271484%2C%22west%22%3A-111.38560346728515%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285743%22%2C%22mapZoom%22%3A12%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95079%2C%22regionType%22%3A7%7D%5D%7D',
  '85747': 'https://www.zillow.com/tucson-az-85747/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.221759997810835%2C%22south%22%3A31.95881650559023%2C%22east%22%3A-110.53549303271485%2C%22west%22%3A-110.98112596728517%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285747%22%2C%22mapZoom%22%3A12%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95083%2C%22regionType%22%3A7%7D%5D%7D',
  '85748': 'https://www.zillow.com/tucson-az-85748/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.28350518993753%2C%22south%22%3A32.1522171757991%2C%22east%22%3A-110.61838426635742%2C%22west%22%3A-110.84120073364258%7D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22price%22%3A%7B%22max%22%3Anull%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22usersSearchTerm%22%3A%2285748%22%2C%22mapZoom%22%3A13%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95084%2C%22regionType%22%3A7%7D%5D%7D',
  '85712': 'https://www.zillow.com/tucson-az-85712/sold/?searchQueryState=%7B%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22north%22%3A32.288060837240906%2C%22south%22%3A32.22244382076419%2C%22east%22%3A-110.8261388831787%2C%22west%22%3A-110.93754711682128%7D%2C%22mapZoom%22%3A14%2C%22usersSearchTerm%22%3A%2285712%22%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C%22fore%22%3A%7B%22value%22%3Afalse%7D%2C%22rs%22%3A%7B%22value%22%3Atrue%7D%2C%22mp%22%3A%7B%22min%22%3A5500%2C%22max%22%3Anull%7D%7D%2C%22isListVisible%22%3Atrue%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A95050%2C%22regionType%22%3A7%7D%5D%7D',
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

// Returns how many days ago a date string was, or null if unparseable.
function soldDaysAgo(soldDateStr) {
  if (!soldDateStr) return null;
  const d = new Date(soldDateStr);
  if (isNaN(d.getTime())) return null;
  return Math.floor((Date.now() - d.getTime()) / (1000 * 60 * 60 * 24));
}

// Returns the numeric sheetId for SHEET_TAB (needed for row deletion).
async function getTrackerSheetId(sheets) {
  const res = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const sheet = res.data.sheets.find(s => s.properties.title === SHEET_TAB);
  if (!sheet) throw new Error(`Sheet tab "${SHEET_TAB}" not found`);
  return sheet.properties.sheetId;
}

// Appends rowValues to ARCHIVE_TAB, then deletes the row from SHEET_TAB.
// rowIndex is 1-based. trackerSheetId is the numeric sheetId.
async function archiveAndDeleteRow(sheets, trackerSheetId, rowIndex, rowValues) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `'${ARCHIVE_TAB}'!A1`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [rowValues] },
  });
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [{
        deleteDimension: {
          range: {
            sheetId: trackerSheetId,
            dimension: 'ROWS',
            startIndex: rowIndex - 1, // 0-based
            endIndex: rowIndex,       // exclusive
          },
        },
      }],
    },
  });
}

async function readAddresses(sheets) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `'${SHEET_TAB}'!A:P`,  // read through P to include stored ZPIDs (col 15)
  });
  const rows = res.data.values || [];
  const result = [];
  for (let i = 2; i < rows.length; i++) {
    const addr = (rows[i][COL.address]  || '').trim();
    const zpid = (rows[i][COL.zpid]     || '').trim();
    if (addr) result.push({
      address:        addr,
      zpid,
      rowIndex:       i + 1,
      storedSoldDate: (rows[i][COL.soldDate]  || '').trim(),
      storedListPrice:(rows[i][COL.listPrice] || '').trim(),
      rowValues:      rows[i],
    });
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

  // Needed for row deletion during graduation; fetch once up front.
  let trackerSheetId;
  try {
    trackerSheetId = await getTrackerSheetId(sheets);
  } catch (err) {
    console.error('Failed to get tracker sheet ID:', err.message);
    return;
  }

  // Rows that meet the graduation threshold are collected here and processed
  // bottom-to-top after all updates, so row-index shifting doesn't affect writes.
  const graduateQueue = []; // { rowIndex, rowValues }

  for (const { address, zpid: storedZpid, rowIndex, storedSoldDate, rowValues } of addressRows) {
    console.log(`\nProcessing [row ${rowIndex}]: ${address}${storedZpid ? ` (ZPID ${storedZpid})` : ' (no ZPID yet)'}`);
    const zip = extractZip(address);

    // ── Fast-path graduation: already known sold > 90 days (skip scraping) ──
    const storedDaysAgo = soldDaysAgo(storedSoldDate);
    if (storedDaysAgo !== null && storedDaysAgo > GRADUATE_AFTER_DAYS) {
      console.log(`  Sold ${storedDaysAgo} days ago (stored) — queued for graduation`);
      graduateQueue.push({ rowIndex, rowValues });
      continue;
    }

    // ── Scrape ──
    let detail = null, zpid = storedZpid, isNewZpid = false;
    try {
      ({ detail, zpid, isNewZpid } = await resolveDetail(address, storedZpid));
      if (detail) console.log(`  Detail: ${detail.streetAddress || detail.addressStreet || address} — ${detail.homeStatus || detail.statusType}`);
      else console.warn('  No detail found');
    } catch (err) {
      console.error(`  Detail error: ${err.message}`);
    }

    const fields = extractFields(detail);

    // ── Post-scrape graduation check ──
    const scrapedDaysAgo = soldDaysAgo(fields.soldDate);
    if (scrapedDaysAgo !== null && scrapedDaysAgo > GRADUATE_AFTER_DAYS) {
      console.log(`  Sold ${scrapedDaysAgo} days ago (scraped) — queued for graduation`);
      // Build archive row with fresh scraped values merged over stored values.
      const archiveRow = [...rowValues];
      while (archiveRow.length <= COL.zpid) archiveRow.push('');
      if (fields.listPrice    !== '') archiveRow[COL.listPrice]    = fields.listPrice;
      if (fields.zillowUrl    !== '') archiveRow[COL.zillowUrl]    = fields.zillowUrl;
      if (fields.listDate     !== '') archiveRow[COL.listDate]     = fields.listDate;
      if (fields.soldPrice    !== '') archiveRow[COL.soldPrice]    = fields.soldPrice;
      if (fields.soldDate     !== '') archiveRow[COL.soldDate]     = fields.soldDate;
      if (fields.daysOnMarket !== '') archiveRow[COL.daysOnMarket] = fields.daysOnMarket;
      if (zpid)                       archiveRow[COL.zpid]         = zpid;
      graduateQueue.push({ rowIndex, rowValues: archiveRow });
      continue;
    }

    // ── Normal update ──
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

    const rowData = { ...fields, zpid: zpid || '', marketAvgDom };

    try {
      await updateSheetRow(sheets, rowIndex, rowData);
      const zpidNote = isNewZpid ? ` (ZPID ${zpid} saved)` : '';
      console.log(`  Sheet row ${rowIndex} updated${zpidNote}`);
    } catch (err) {
      console.error(`  Sheet write error: ${err.message}`);
    }
  }

  // ── Graduate: archive then delete, bottom-to-top to preserve row indices ──
  if (graduateQueue.length) {
    console.log(`\nGraduating ${graduateQueue.length} row(s) to "${ARCHIVE_TAB}"...`);
    graduateQueue.sort((a, b) => b.rowIndex - a.rowIndex);
    for (const { rowIndex, rowValues: archiveRow } of graduateQueue) {
      try {
        await archiveAndDeleteRow(sheets, trackerSheetId, rowIndex, archiveRow);
        console.log(`  Row ${rowIndex} archived and deleted`);
      } catch (err) {
        console.error(`  Graduate error (row ${rowIndex}): ${err.message}`);
      }
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

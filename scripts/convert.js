// scripts/convert.js
// Usage:
//   node scripts/convert.js <google_feed_url> > public/yandex.yml
//
// Особенности v2:
// - Поддержка Atom <feed>/<entry> с namespace g:* (стрип префиксов)
// - Корректная обработка price/sale_price (KZT), availability, condition
// - Картинки: нормализация абсолютного URL (если начинается с '/')
// - Категории из product_type/google_product_category
// - barcode из gtin, vendorCode из mpn
// - oldprice -> из price, price -> из sale_price (если есть)
// - Простые delivery-options (0 KZT, "1-3" дней) — при наличии shipping
//
// Переменные окружения (опционально):
//   BASE_IMAGE_ORIGIN=https://images.samsung.com  # для относительных image_link
//   SHOP_NAME="Samsung KZ" SHOP_COMPANY="Samsung" SHOP_URL="https://shop.samsung.com/kz_ru"

const fetch = require('node-fetch');
const xml2js = require('xml2js');
const { stripPrefix } = require('xml2js').processors;

const FEED_URL = process.argv[2] || process.env.GOOGLE_FEED_URL;
if (!FEED_URL) {
  console.error('Usage: node scripts/convert.js <google_feed_url>');
  process.exit(2);
}

const SHOP_NAME    = process.env.SHOP_NAME    || 'My Shop';
const SHOP_COMPANY = process.env.SHOP_COMPANY || 'My Company';
const SHOP_URL     = process.env.SHOP_URL     || 'https://example.com';
const BASE_IMAGE_ORIGIN = process.env.BASE_IMAGE_ORIGIN || '';

const ALLOWED_CURRENCIES = new Set(['RUR','RUB','KZT','USD','EUR','BYN','UAH']);
function normCurrency(cur = 'KZT') {
  let c = String(cur || 'KZT').toUpperCase();
  if (c === 'RUB') c = 'RUR';
  return ALLOWED_CURRENCIES.has(c) ? c : 'KZT';
}

function escapeXml(str='') {
  return String(str).replace(/[<>&'"]/g, c =>
    ({'<':'&lt;','>':'&gt;','&':'&amp;',"'":'&apos;','"':'&quot;'}[c])
  );
}
const trim1 = s => String(s || '').trim();
const one = v => Array.isArray(v) ? v[0] : v;

function parsePriceToken(raw='') {
  // "359990 KZT" | "359990" | "359,990 KZT"
  const m = String(raw).trim().match(/^([\d\s.,]+)\s*([A-Za-z]{3})?$/);
  if (!m) return { amount: '', currency: 'KZT' };
  const amount = m[1].replace(/\s/g,'').replace(',','.');
  const currency = normCurrency(m[2] || 'KZT');
  return { amount, currency };
}

function asArray(x) { return Array.isArray(x) ? x : (x ? [x] : []); }

function fullImageUrl(s) {
  const u = trim1(s);
  if (!u) return '';
  if (/^https?:\/\//i.test(u)) return u;
  if (u.startsWith('/') && BASE_IMAGE_ORIGIN) return BASE_IMAGE_ORIGIN.replace(/\/+$/,'') + u;
  return u; // fallback: как есть
}

function pickCategory(entry) {
  const a = trim1(entry.product_type || '');
  const b = trim1(entry.google_product_category || '');
  return a || b || 'Default';
}

function mapAvailability(av) {
  const x = trim1(av).toLowerCase();
  if (x === 'in_stock' || x === 'preorder' || x === 'available for order') return 'true';
  if (x === 'out_of_stock' || x === 'sold_out') return 'false';
  return 'true';
}

function mapCondition(cnd) {
  const x = trim1(cnd).toLowerCase();
  if (x === 'new' || x === 'brand new') return 'new';
  if (x === 'used' || x === 'refurbished') return x;
  return 'new';
}

async function main() {
  const res = await fetch(FEED_URL);
  if (!res.ok) throw new Error(`Fetch failed: ${res.status} ${res.statusText}`);
  const xml = await res.text();

  const parser = new xml2js.Parser({
    explicitArray: false,
    mergeAttrs: true,
    tagNameProcessors: [stripPrefix], // снимаем g:/atom префиксы
    valueProcessors: [trim1],
    attrValueProcessors: [trim1],
  });

  const root = await parser.parseStringPromise(xml);

  // Atom feed → entries
  const entriesRaw = root?.feed?.entry;
  const entries = entriesRaw ? (Array.isArray(entriesRaw) ? entriesRaw : [entriesRaw]) : [];

  // Соберём офферы
  const offers = entries.map(e => {
    const id   = one(e.id) || one(e.gid) || Math.random().toString(36).slice(2,9);
    const name = trim1(one(e.title));
    const description = trim1(one(e.description));
    const url  = trim1(one(e.link)) || trim1(one(e.link?.href)) || '';
    // image_link может быть массивом/многострочным
    const img  = fullImageUrl(one(e.image_link || e['image link'] || ''));
    const brand = trim1(one(e.brand));
    const mpn   = trim1(one(e.mpn));
    const gtin  = trim1(one(e.gtin));
    const availability = mapAvailability(one(e.availability));
    const condition    = mapCondition(one(e.condition));

    // цены
    const pricePrimary = parsePriceToken(one(e.price || ''));
    const sale = parsePriceToken(one(e.sale_price || ''));
    // YML: price — текущая цена, oldprice — старая
    let price = pricePrimary.amount;
    let currencyId = pricePrimary.currency;
    let oldprice = '';

    if (sale.amount) {
      oldprice = pricePrimary.amount || '';
      price = sale.amount;
      currencyId = sale.currency || currencyId;
    }
    currencyId = normCurrency(currencyId || 'KZT');

    // категории
    const category = pickCategory({
      product_type: e.product_type,
      google_product_category: e.google_product_category
    });

    // shipping → для демонстрации добавим <delivery-options><option cost="0" days="1-3"/>
    const hasShipping = asArray(e.shipping).length > 0;

    return {
      id, name, description, url, picture: img, brand, mpn, gtin,
      availability, condition, price, oldprice, currencyId, category,
      hasShipping
    };
  });

  // Категории (уникальные)
  const categoriesMap = new Map();
  let catSeq = 1;
  for (const o of offers) {
    const key = o.category || 'Default';
    if (!categoriesMap.has(key)) categoriesMap.set(key, String(catSeq++));
  }

  const now = new Date().toISOString().replace('T',' ').replace(/\..+/, '');
  let out = `<?xml version="1.0" encoding="UTF-8"?>\n`;
  out += `<yml_catalog date="${now}">\n  <shop>\n`;
  out += `    <name>${escapeXml(SHOP_NAME)}</name>\n`;
  out += `    <company>${escapeXml(SHOP_COMPANY)}</company>\n`;
  out += `    <url>${escapeXml(SHOP_URL)}</url>\n`;

  // Валюты
  const currencies = new Set(offers.map(o => o.currencyId));
  out += `    <currencies>\n`;
  for (const c of currencies) out += `      <currency id="${c}" rate="1"/>\n`;
  out += `    </currencies>\n`;

  // Категории
  out += `    <categories>\n`;
  for (const [name, cid] of categoriesMap) {
    out += `      <category id="${cid}">${escapeXml(name)}</category>\n`;
  }
  out += `    </categories>\n`;

  // Офферы
  out += `    <offers>\n`;
  for (const o of offers) {
    const cid = categoriesMap.get(o.category) || '1';
    out += `      <offer id="${escapeXml(o.id)}" available="${o.availability}">\n`;
    if (o.url)     out += `        <url>${escapeXml(o.url)}</url>\n`;
    if (o.price)   out += `        <price>${escapeXml(o.price)}</price>\n`;
    if (o.oldprice)out += `        <oldprice>${escapeXml(o.oldprice)}</oldprice>\n`;
    out += `        <currencyId>${o.currencyId}</currencyId>\n`;
    out += `        <categoryId>${cid}</categoryId>\n`;
    if (o.picture) out += `        <picture>${escapeXml(o.picture)}</picture>\n`;
    if (o.brand)   out += `        <vendor>${escapeXml(o.brand)}</vendor>\n`;
    if (o.mpn)     out += `        <vendorCode>${escapeXml(o.mpn)}</vendorCode>\n`;
    if (o.gtin)    out += `        <barcode>${escapeXml(o.gtin)}</barcode>\n`;
    if (o.name)    out += `        <name>${escapeXml(o.name)}</name>\n`;
    if (o.description) out += `        <description>${escapeXml(o.description)}</description>\n`;
    if (o.condition) out += `        <condition>${escapeXml(o.condition)}</condition>\n`;
    if (o.hasShipping) {
      out += `        <delivery-options>\n`;
      out += `          <option cost="0" days="1-3"/>\n`;
      out += `        </delivery-options>\n`;
    }
    out += `      </offer>\n`;
  }
  out += `    </offers>\n  </shop>\n</yml_catalog>\n`;

  process.stdout.write(out);
}

main().catch(err => { console.error(err); process.exit(1); });
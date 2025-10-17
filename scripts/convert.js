// scripts/convert.js
// Usage: node scripts/convert.js <google_feed_url> > public/yandex.yml
const fetch = require('node-fetch');
const xml2js = require('xml2js');

const FEED_URL = process.argv[2] || process.env.GOOGLE_FEED_URL;
if (!FEED_URL) {
  console.error('Usage: node scripts/convert.js <google_feed_url>');
  process.exit(2);
}

const ALLOWED_CURRENCIES = new Set(['RUR','RUB','KZT','USD','EUR','BYN','UAH']);

function escapeXml(str='') {
  return String(str).replace(/[<>&'"]/g, c =>
    ({'<':'&lt;','>':'&gt;','&':'&amp;',"'":'&apos;','"':'&quot;'}[c])
  );
}

function pick(val) { return Array.isArray(val) ? val[0] : val; }

function parsePrice(raw='') {
  // Примеры: "123.45 USD" или "123.45"
  const m = String(raw).trim().match(/^([\d.,]+)\s*([A-Za-z]{3})?$/);
  if (!m) return {price:'', currencyId:'KZT'};
  const price = m[1].replace(',', '.');
  let currencyId = (m[2] || 'KZT').toUpperCase();
  if (currencyId === 'RUB') currencyId = 'RUR'; // Яндекс допускает RUR
  if (!ALLOWED_CURRENCIES.has(currencyId)) currencyId = 'KZT';
  return {price, currencyId};
}

async function main() {
  const res = await fetch(FEED_URL);
  if (!res.ok) throw new Error(`Fetch failed: ${res.status} ${res.statusText}`);
  const text = await res.text();

  const parser = new xml2js.Parser({explicitArray:false, mergeAttrs:true});
  const g = await parser.parseStringPromise(text);

  // Поддержка Google Merchant (rss/channel/item с g:* полями)
  const itemsRaw = g?.rss?.channel?.item;
  const items = !itemsRaw ? [] : (Array.isArray(itemsRaw) ? itemsRaw : [itemsRaw]);

  const offers = items.map(it => {
    // Частые поля Google: title, link, description, g:image_link, g:price, g:brand, g:id
    const id = pick(it['g:id'] || it['id'] || it['guid']?._) || Math.random().toString(36).slice(2,9);
    const url = pick(it.link) || '';
    const name = pick(it.title) || '';
    const description = (pick(it.description) || '').replace(/\s+/g,' ').trim();
    const picture = pick(it['g:image_link'] || it['g:additional_image_link']) || '';
    const brand = pick(it['g:brand']) || '';
    const category = pick(it['g:product_type']) || 'Default';
    const priceRaw = pick(it['g:price'] || it['price'] || '');
    const {price, currencyId} = parsePrice(priceRaw);

    return {id, url, name, description, picture, brand, category, price, currencyId};
  });

  const now = new Date().toISOString().replace('T',' ').replace(/\..+/, '');
  let out = `<?xml version="1.0" encoding="UTF-8"?>\n`;
  out += `<yml_catalog date="${now}">\n  <shop>\n`;
  out += `    <name>My Shop</name>\n    <company>My Company</company>\n    <url>https://example.com</url>\n`;

  // Простейший раздел валют/категорий (добавь свои при необходимости)
  const currencies = new Set(offers.map(o=>o.currencyId));
  out += `    <currencies>\n`;
  for (const cur of currencies) out += `      <currency id="${cur}" rate="1"/>\n`;
  out += `    </currencies>\n`;

  const categoriesMap = new Map();
  let catIdSeq = 1;
  for (const o of offers) {
    if (!categoriesMap.has(o.category)) categoriesMap.set(o.category, String(catIdSeq++));
  }
  out += `    <categories>\n`;
  for (const [name, cid] of categoriesMap) out += `      <category id="${cid}">${escapeXml(name)}</category>\n`;
  out += `    </categories>\n`;

  out += `    <offers>\n`;
  for (const o of offers) {
    const categoryId = categoriesMap.get(o.category) || '1';
    out += `      <offer id="${escapeXml(o.id)}" available="true">\n`;
    out += `        <url>${escapeXml(o.url)}</url>\n`;
    if (o.price) out += `        <price>${escapeXml(o.price)}</price>\n`;
    out += `        <currencyId>${o.currencyId}</currencyId>\n`;
    out += `        <categoryId>${categoryId}</categoryId>\n`;
    if (o.picture) out += `        <picture>${escapeXml(o.picture)}</picture>\n`;
    if (o.brand) out += `        <vendor>${escapeXml(o.brand)}</vendor>\n`;
    out += `        <name>${escapeXml(o.name)}</name>\n`;
    out += `        <description>${escapeXml(o.description)}</description>\n`;
    out += `      </offer>\n`;
  }
  out += `    </offers>\n  </shop>\n</yml_catalog>\n`;

  process.stdout.write(out);
}

main().catch(err => { console.error(err); process.exit(1); });
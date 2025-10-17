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
const SHOP_ORIGIN = (() => {
  try {
    return new URL(SHOP_URL).origin;
  } catch {
    return '';
  }
})();

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

const HTML_ENTITIES = {
  amp: '&',
  lt: '<',
  gt: '>',
  quot: '"',
  apos: "'",
  nbsp: ' ',
};

const CP1252_EXT = new Map([
  [0x20ac, 0x80], [0x201a, 0x82], [0x0192, 0x83], [0x201e, 0x84],
  [0x2026, 0x85], [0x2020, 0x86], [0x2021, 0x87], [0x02c6, 0x88],
  [0x2030, 0x89], [0x0160, 0x8a], [0x2039, 0x8b], [0x0152, 0x8c],
  [0x017d, 0x8e], [0x2018, 0x91], [0x2019, 0x92], [0x201c, 0x93],
  [0x201d, 0x94], [0x2022, 0x95], [0x2013, 0x96], [0x2014, 0x97],
  [0x02dc, 0x98], [0x2122, 0x99], [0x0161, 0x9a], [0x203a, 0x9b],
  [0x0153, 0x9c], [0x017e, 0x9e], [0x0178, 0x9f],
]);

function decodeBrokenUtf8(str='') {
  if (!/[ÐÑ]/.test(str)) return str;
  const bytes = [];
  for (const ch of str) {
    const code = ch.codePointAt(0);
    if (code <= 0xff) {
      bytes.push(code);
      continue;
    }
    const mapped = CP1252_EXT.get(code);
    if (mapped == null) return str;
    bytes.push(mapped);
  }
  try {
    const decoded = Buffer.from(bytes).toString('utf8');
    return /[А-Яа-яЁё]/.test(decoded) ? decoded : str;
  } catch {
    return str;
  }
}

function decodeHtml(str = '') {
  return String(str).replace(/&(#x?[0-9a-f]+|\w+);/gi, (m, entity) => {
    if (entity[0] === '#') {
      const code = entity[1].toLowerCase() === 'x'
        ? parseInt(entity.slice(2), 16)
        : parseInt(entity.slice(1), 10);
      return Number.isFinite(code) ? String.fromCodePoint(code) : m;
    }
    const key = entity.toLowerCase();
    return Object.prototype.hasOwnProperty.call(HTML_ENTITIES, key) ? HTML_ENTITIES[key] : m;
  });
}

const stripHtml = (s = '') => String(s || '').replace(/<\/?[^>]+>/g, ' ');
const collapseWs = (s = '') => String(s || '').replace(/\s+/g, ' ').trim();
const cleanText = (s = '') => collapseWs(decodeBrokenUtf8(decodeHtml(s)));

function normalizeDescription(raw = '') {
  const text = collapseWs(decodeBrokenUtf8(stripHtml(decodeHtml(raw))));
  if (!text) return '';
  const max = 3000;
  if (text.length <= max) return text;
  const truncated = text.slice(0, max - 1).replace(/\s+\S*$/, '').trim();
  return truncated ? `${truncated}…` : text.slice(0, max - 1);
}

function parsePriceToken(raw='') {
  // "359990 KZT" | "359990" | "359,990 KZT"
  const m = String(raw).trim().match(/^([\d\s.,]+)\s*([A-Za-z]{3})?$/);
  if (!m) return { amount: '', currency: 'KZT' };
  const amount = m[1].replace(/\s/g,'').replace(',','.');
  const currency = normCurrency(m[2] || 'KZT');
  return { amount, currency };
}

function asArray(x) { return Array.isArray(x) ? x : (x ? [x] : []); }

function ensureAbsolute(url, base) {
  if (!base) return '';
  try {
    return new URL(url, base).toString();
  } catch {
    return '';
  }
}

function fullImageUrl(s) {
  const u = trim1(s);
  if (!u) return '';
  if (/^https?:\/\//i.test(u)) {
    try {
      return new URL(u).toString();
    } catch {
      return '';
    }
  }
  if (u.startsWith('//')) {
    try {
      return new URL(`https:${u}`).toString();
    } catch {
      return '';
    }
  }
  const resolved =
    ensureAbsolute(u, BASE_IMAGE_ORIGIN) ||
    ensureAbsolute(u, SHOP_URL) ||
    ensureAbsolute(u, SHOP_ORIGIN);
  return resolved;
}

function pickCategory(entry) {
  const a = cleanText(entry.product_type || '');
  const b = cleanText(entry.google_product_category || '');
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
  if (!x || x === 'new' || x === 'brand new') return null;

  if (x === 'used' || x === 'second hand' || x === 'б/у') {
    return { type: 'used', quality: 'good' };
  }
  if (x === 'like new' || x === 'as new' || x === 'open box') {
    return { type: 'likenew', quality: 'excellent' };
  }
  if (x === 'refurbished' || x === 'renewed' || x === 'preowned') {
    return { type: 'preowned', quality: 'refurbished' };
  }
  if (x === 'showcase' || x === 'demo' || x === 'display') {
    return { type: 'showcasesample', quality: 'good' };
  }
  if (x === 'reduction' || x === 'discounted') {
    return { type: 'reduction', quality: 'good' };
  }

  return null;
}

const PARAM_FIELDS = [
  { key: 'color', label: 'Цвет' },
  { key: 'material', label: 'Материал' },
  { key: 'size', label: 'Размер' },
  { key: 'size_type', label: 'Тип размера' },
  { key: 'size_system', label: 'Размерная сетка' },
  { key: 'pattern', label: 'Принт' },
  { key: 'gender', label: 'Пол' },
  { key: 'age_group', label: 'Возраст' },
  { key: 'capacity', label: 'Объем' },
  { key: 'power', label: 'Мощность' },
  { key: 'voltage', label: 'Напряжение' },
  { key: 'width', label: 'Ширина' },
  { key: 'height', label: 'Высота' },
  { key: 'depth', label: 'Глубина' },
];

for (let i = 0; i <= 4; i += 1) {
  PARAM_FIELDS.push({ key: `custom_label_${i}`, label: `Метка ${i}` });
}

function dedupeParams(list) {
  const seen = new Set();
  return list.filter(({ name, value }) => {
    const key = `${name}:::${value}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function resolveParams(entry) {
  const params = [];

  for (const { key, label } of PARAM_FIELDS) {
    const value = cleanText(one(entry[key]));
    if (value) params.push({ name: label || key, value });
  }

  const details = asArray(entry.product_detail || entry.product_details);
  for (const detail of details) {
    const name = cleanText(one(detail?.attribute_name) || one(detail?.name) || one(detail?.title));
    const value = cleanText(one(detail?.attribute_value) || one(detail?.value) || one(detail?.description));
    if (name && value) params.push({ name, value });
  }

  const explicitParams = asArray(entry.param || entry.parameter);
  for (const param of explicitParams) {
    const name = cleanText(param?.name || param?.$?.name || one(param?.name));
    const value = cleanText(param?.value || one(param?.value) || param?._);
    if (name && value) params.push({ name, value });
  }

  return dedupeParams(params);
}

function collectPictures(entry) {
  const pool = [
    ...asArray(entry.image_link),
    ...asArray(entry['image link']),
    ...asArray(entry.additional_image_link),
    ...asArray(entry['additional_image_link']),
    ...asArray(entry.image_links),
  ];

  const urls = pool
    .flatMap(item => {
      if (!item) return [];
      if (Array.isArray(item)) return item;
      const parts = String(item).split(/[\s\n\r]+/);
      return parts.filter(Boolean);
    })
    .map(cleanText)
    .map(fullImageUrl)
    .filter(Boolean);

  const seen = new Set();
  return urls.filter(u => {
    if (seen.has(u)) return false;
    seen.add(u);
    return true;
  });
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
    const rawId = cleanText(one(e.id) || one(e.gid));
    const id   = rawId || Math.random().toString(36).slice(2,9);
    const name = cleanText(one(e.title));
    const descriptionSource = one(e.description) || one(e.summary) || one(e.content);
    const description = normalizeDescription(descriptionSource);
    const url  = trim1(one(e.link)) || trim1(one(e.link?.href)) || '';
    const pictures = collectPictures(e);
    const brand = cleanText(one(e.brand));
    const mpn   = cleanText(one(e.mpn));
    const gtin  = cleanText(one(e.gtin));
    const skuRaw = cleanText(one(e.sku));
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
    currencyId = 'KZT';

    // категории
    const category = pickCategory({
      product_type: e.product_type,
      google_product_category: e.google_product_category
    });

    // shipping → для демонстрации добавим <delivery-options><option cost="0" days="1-3"/>
    const hasShipping = asArray(e.shipping).length > 0;

    const itemGroupId = cleanText(one(e.item_group_id));
    const params = resolveParams(e);
    const shopSku = skuRaw || (itemGroupId ? `${itemGroupId}-${id}` : id);

    let offer = {
      id, name, description, url, pictures, brand, mpn, gtin,
      availability, condition, price, oldprice, currencyId, category,
      hasShipping, itemGroupId, params, shopSku
    };

    const priceNum = Number(price);
    if (!price || Number.isNaN(priceNum) || priceNum <= 0) {
      const fallback = Number(oldprice);
      if (fallback > 0) {
        offer.price = String(oldprice);
        offer.oldprice = '';
      } else {
        // пропускаем оффер с некорректной ценой
        return null;
      }
    }
    if (!offer.pictures.length) delete offer.pictures;

    return offer;
  }).filter(Boolean);

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
  out += `    <currencies>\n`;
  out += `      <currency id="KZT" rate="1"/>\n`;
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
    const attrParts = [
      `id="${escapeXml(o.id)}"`,
      `available="${o.availability}"`,
    ];
    if (o.itemGroupId) {
      attrParts.push(`group_id="${escapeXml(o.itemGroupId)}"`);
    }
    out += `      <offer ${attrParts.join(' ')}>\n`;
    if (o.url)     out += `        <url>${escapeXml(o.url)}</url>\n`;
    if (o.price)   out += `        <price>${escapeXml(o.price)}</price>\n`;
    if (o.oldprice)out += `        <oldprice>${escapeXml(o.oldprice)}</oldprice>\n`;
    out += `        <currencyId>KZT</currencyId>\n`;
    out += `        <categoryId>${cid}</categoryId>\n`;
    if (o.pictures?.length) {
      for (const pic of o.pictures) {
        out += `        <picture>${escapeXml(pic)}</picture>\n`;
      }
    }
    if (o.brand)   out += `        <vendor>${escapeXml(o.brand)}</vendor>\n`;
    if (o.mpn)     out += `        <vendorCode>${escapeXml(o.mpn)}</vendorCode>\n`;
    if (o.gtin)    out += `        <barcode>${escapeXml(o.gtin)}</barcode>\n`;
    if (o.shopSku) out += `        <shop-sku>${escapeXml(o.shopSku)}</shop-sku>\n`;
    if (o.name)    out += `        <name>${escapeXml(o.name)}</name>\n`;
    if (o.description) out += `        <description>${escapeXml(o.description)}</description>\n`;
    if (o.condition?.type && o.condition?.quality) {
      out += `        <condition type="${escapeXml(o.condition.type)}">\n`;
      out += `          <quality>${escapeXml(o.condition.quality)}</quality>\n`;
      out += `        </condition>\n`;
    }
    if (o.params?.length) {
      for (const param of o.params) {
        out += `        <param name="${escapeXml(param.name)}">${escapeXml(param.value)}</param>\n`;
      }
    }
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

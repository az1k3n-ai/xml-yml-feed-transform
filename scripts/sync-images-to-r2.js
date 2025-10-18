#!/usr/bin/env node

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const fetch = require('node-fetch');
const sharp = require('sharp');
const { S3Client, HeadObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

const {
  CF_ACCOUNT_ID,
  CF_R2_ACCESS_KEY_ID,
  CF_R2_SECRET_ACCESS_KEY,
  R2_BUCKET,
  R2_PUBLIC_BASE,
} = process.env;

const CONCURRENCY = Math.max(1, Number(process.env.R2_CONCURRENCY) || 4);
const MAX_FETCH_ATTEMPTS = Math.max(1, Number(process.env.R2_FETCH_ATTEMPTS) || 3);

const IMAGES_LIST_PATH = path.resolve('images.json');
const LOCAL_R2_MANIFEST_PATH = path.resolve('public/manifest-r2.json');
const LOCAL_IMAGE_MANIFEST_PATH = path.resolve('public/images-manifest.json');
const R2_MANIFEST_KEY = 'manifests/images-manifest.json';

const MIME_TO_EXT = new Map([
  ['image/jpeg', 'jpg'],
  ['image/jpg', 'jpg'],
  ['image/png', 'png'],
  ['image/webp', 'webp'],
  ['image/gif', 'gif'],
]);
const SUPPORTED_MIME = new Set(MIME_TO_EXT.keys());

function assertEnv(name, value) {
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

async function readJsonSafe(filePath, fallback) {
  try {
    const data = await fs.promises.readFile(filePath, 'utf8');
    return JSON.parse(data);
  } catch (err) {
    if (err.code === 'ENOENT') return fallback;
    throw err;
  }
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function normaliseMime(mime) {
  if (!mime) return 'image/jpeg';
  const clean = mime.split(';')[0].trim().toLowerCase();
  if (SUPPORTED_MIME.has(clean)) return clean;
  return clean.startsWith('image/') ? clean : 'image/jpeg';
}

async function resizeBuffer(inputBuffer, mime) {
  const targetMime = SUPPORTED_MIME.has(mime) ? mime : 'image/jpeg';
  const ext = MIME_TO_EXT.get(targetMime) || 'jpg';
  const image = sharp(inputBuffer, { animated: targetMime === 'image/gif' });
  const pipeline = image.resize({ width: 1600, withoutEnlargement: true, fit: 'inside' });

  switch (ext) {
    case 'png':
      return { buffer: await pipeline.png().toBuffer(), mime: 'image/png', ext: 'png' };
    case 'webp':
      return { buffer: await pipeline.webp().toBuffer(), mime: 'image/webp', ext: 'webp' };
    case 'gif':
      return { buffer: await pipeline.gif().toBuffer(), mime: 'image/gif', ext: 'gif' };
    default:
      return { buffer: await pipeline.jpeg({ quality: 90 }).toBuffer(), mime: 'image/jpeg', ext: 'jpg' };
  }
}

async function headR2(s3, key) {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: key }));
    return true;
  } catch (err) {
    if (err.$metadata?.httpStatusCode === 404 || err.name === 'NotFound') return false;
    throw err;
  }
}

async function putR2(s3, key, body, contentType) {
  await s3.send(new PutObjectCommand({
    Bucket: R2_BUCKET,
    Key: key,
    Body: body,
    ContentType: contentType,
    CacheControl: 'public, max-age=31536000, immutable',
  }));
}

async function putManifestToR2(s3, manifestString) {
  await s3.send(new PutObjectCommand({
    Bucket: R2_BUCKET,
    Key: R2_MANIFEST_KEY,
    Body: Buffer.from(manifestString),
    ContentType: 'application/json',
    CacheControl: 'no-cache',
  }));
}

function sortObjectDeep(value) {
  if (Array.isArray(value)) return value.map(sortObjectDeep);
  if (value && typeof value === 'object') {
    return Object.keys(value).sort().reduce((acc, key) => {
      acc[key] = sortObjectDeep(value[key]);
      return acc;
    }, {});
  }
  return value;
}

async function processOffer(entry, context) {
  const { publicBase, previousManifest, nextManifest, s3, stats } = context;
  const { offerId, urls } = entry || {};
  if (!offerId || !Array.isArray(urls) || !urls.length) return null;

  const uniqueUrls = Array.from(new Set(urls.map(u => String(u).trim()).filter(Boolean)));
  if (!uniqueUrls.length) return null;

  const resolvedUrls = [];

  for (const srcUrl of uniqueUrls) {
    if (nextManifest[srcUrl]?.r2Key) {
      resolvedUrls.push(`${publicBase}/${nextManifest[srcUrl].r2Key}`);
      continue;
    }

    const prevMeta = nextManifest[srcUrl] || previousManifest[srcUrl] || {};
    let attempt = 0;
    let lastError = null;
    let conditional = Boolean(prevMeta.etag || prevMeta.lastModified);

    while (attempt < MAX_FETCH_ATTEMPTS) {
      attempt += 1;
      lastError = null;
      try {
        const headers = {};
        if (conditional) {
          if (prevMeta.etag) headers['If-None-Match'] = prevMeta.etag;
          if (prevMeta.lastModified) headers['If-Modified-Since'] = prevMeta.lastModified;
        }

        const response = await fetch(srcUrl, { headers, redirect: 'follow' });

        if (response.status === 304) {
          if (prevMeta.r2Key) {
            stats.reused304 += 1;
            resolvedUrls.push(`${publicBase}/${prevMeta.r2Key}`);
            nextManifest[srcUrl] = { ...prevMeta };
            break;
          }
          conditional = false;
          continue;
        }

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const etag = response.headers.get('etag') || undefined;
        const lastModified = response.headers.get('last-modified') || undefined;
        const contentType = normaliseMime(response.headers.get('content-type'));
        const arrayBuffer = await response.arrayBuffer();
        const sourceBuffer = Buffer.from(arrayBuffer);

        const processed = await resizeBuffer(sourceBuffer, contentType);
        const sha1 = crypto.createHash('sha1').update(processed.buffer).digest('hex');
        const key = `img/${sha1}.${processed.ext}`;

        let exists = false;
        try {
          exists = await headR2(s3, key);
        } catch (err) {
          throw new Error(`HEAD failed: ${err.message}`);
        }

        if (!exists) {
          try {
            await putR2(s3, key, processed.buffer, processed.mime);
            stats.uploaded += 1;
          } catch (err) {
            throw new Error(`Upload failed: ${err.message}`);
          }
        } else {
          stats.reusedExisting += 1;
        }

        resolvedUrls.push(`${publicBase}/${key}`);
        nextManifest[srcUrl] = {
          etag,
          lastModified,
          r2Key: key,
          mime: processed.mime,
        };
        break;
      } catch (err) {
        lastError = err;
        if (attempt >= MAX_FETCH_ATTEMPTS) break;
        await sleep(200 * Math.pow(2, attempt - 1));
        conditional = false;
      }
    }

    if (lastError) {
      stats.skipped += 1;
      console.warn(`Skip ${srcUrl}: ${lastError.message}`);
    }
  }

  if (resolvedUrls.length) {
    return { offerId, urls: resolvedUrls };
  }
  return null;
}

async function main() {
  assertEnv('CF_ACCOUNT_ID', CF_ACCOUNT_ID);
  assertEnv('CF_R2_ACCESS_KEY_ID', CF_R2_ACCESS_KEY_ID);
  assertEnv('CF_R2_SECRET_ACCESS_KEY', CF_R2_SECRET_ACCESS_KEY);
  assertEnv('R2_BUCKET', R2_BUCKET);
  const publicBase = assertEnv('R2_PUBLIC_BASE', R2_PUBLIC_BASE).replace(/\/+$/, '');

  if (!fs.existsSync(IMAGES_LIST_PATH)) {
    throw new Error(`images list not found: ${IMAGES_LIST_PATH}`);
  }

  const imagesList = await readJsonSafe(IMAGES_LIST_PATH, []);
  if (!Array.isArray(imagesList) || !imagesList.length) {
    console.log('No images to process.');
    await fs.promises.mkdir(path.dirname(LOCAL_R2_MANIFEST_PATH), { recursive: true });
    await fs.promises.writeFile(LOCAL_R2_MANIFEST_PATH, '[]', 'utf8');
    await fs.promises.writeFile(LOCAL_IMAGE_MANIFEST_PATH, '{}', 'utf8');
    return;
  }

  await fs.promises.mkdir(path.dirname(LOCAL_R2_MANIFEST_PATH), { recursive: true });

  const previousManifest = await readJsonSafe(LOCAL_IMAGE_MANIFEST_PATH, {});
  const nextManifest = {};

  const s3 = new S3Client({
    region: 'auto',
    endpoint: `https://${CF_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: CF_R2_ACCESS_KEY_ID,
      secretAccessKey: CF_R2_SECRET_ACCESS_KEY,
    },
  });

  const stats = { uploaded: 0, reused304: 0, reusedExisting: 0, skipped: 0 };
  const offerResults = [];

  let index = 0;
  const workerCount = Math.min(CONCURRENCY, imagesList.length);
  const workers = Array.from({ length: workerCount }, async () => {
    while (true) {
      const current = index++;
      if (current >= imagesList.length) break;
      const offer = imagesList[current];
      try {
        const result = await processOffer(offer, {
          publicBase,
          previousManifest,
          nextManifest,
          s3,
          stats,
        });
        if (result) offerResults.push(result);
      } catch (err) {
        stats.skipped += 1;
        console.warn(`Offer ${offer?.offerId || 'unknown'} failed: ${err.message}`);
      }
    }
  });

  await Promise.all(workers);

  const sortedOffers = offerResults
    .map(({ offerId, urls }) => ({
      offerId,
      urls: Array.from(new Set(urls)).sort(),
    }))
    .sort((a, b) => a.offerId.localeCompare(b.offerId));

  const sortedManifest = sortObjectDeep(nextManifest);
  const previousManifestSorted = sortObjectDeep(previousManifest);
  const manifestString = JSON.stringify(sortedManifest, null, 2);
  const previousManifestString = JSON.stringify(previousManifestSorted, null, 2);
  const manifestChanged = manifestString !== previousManifestString;

  await fs.promises.writeFile(LOCAL_R2_MANIFEST_PATH, JSON.stringify(sortedOffers, null, 2), 'utf8');
  await fs.promises.writeFile(LOCAL_IMAGE_MANIFEST_PATH, manifestString, 'utf8');

  if (manifestChanged) {
    try {
      await putManifestToR2(s3, manifestString);
    } catch (err) {
      console.warn(`Failed to upload manifest to R2: ${err.message}`);
    }
  }

  console.log(
    `Images processed: offers=${sortedOffers.length}, uploaded=${stats.uploaded}, reused(304)=${stats.reused304}, reused(existing)=${stats.reusedExisting}, skipped=${stats.skipped}, concurrency=${workerCount}`
  );
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});

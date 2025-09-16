// index.js
// package.json -> { "type": "module" }
// npm i crawlee slugify
// Windows: $env:CRAWLEE_SYSTEM_INFO_V2='1'; node index.js

import { CheerioCrawler, log, Configuration } from 'crawlee';
import slugifyLib from 'slugify';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import crypto from 'crypto';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const CINEMA_URL = process.env.CINEMA_URL || 'https://almazcinema.com/ijv/cinema/53/schedule/';
const IMAGES_DIR = process.env.IMAGES_DIR || path.join(__dirname, 'images');

const config = new Configuration({ systemInfoV2: true });

// ---------- утилиты ----------
function slugify(input) {
  return slugifyLib(input, { lower: true, strict: true, trim: true });
}
function toAbsUrl(maybeUrl, base) {
  try { return new URL(maybeUrl, base).toString(); }
  catch {
    if (maybeUrl?.startsWith('/')) return (new URL(base)).origin + maybeUrl;
    return maybeUrl || base;
  }
}
// Удаляет markdown [текст](url) -> url, чистит мусор
function stripMarkdownUrl(s) {
  const str = String(s || '').trim();
  const md = str.match(/\((https?:\/\/[^\s)]+)\)/);
  if (md && md[12]) return md[12];
  const link = str.match(/https?:\/\/[^\s,]+/);
  return link ? link : str.replace(/^[\s,]+|[\s,]+$/g, '');
}
// Жёсткая нормализация к единственному абсолютному URL
function normalizeOneUrl(u, base) {
  const raw = stripMarkdownUrl(u);
  if (!raw) return '';
  try {
    const abs = new URL(raw, base);
    // origin + pathname, сохраняем завершающий слэш, убираем query/fragment
    const pathname = abs.pathname.endsWith('/') ? abs.pathname : abs.pathname + '/';
    return `${abs.origin}${pathname}`;
  } catch {
    return '';
  }
}
// Выбор ЛУЧШЕГО кандидата (отдаём один URL)
function pickBestMovieUrl(candidates, base) {
  // нормализуем, фильтруем пустые, убираем дубли
  const seen = new Set();
  const urls = [];
  for (const c of candidates) {
    const v = normalizeOneUrl(c, base);
    if (v && !seen.has(v)) {
      seen.add(v);
      urls.push(v);
    }
  }
  if (urls.length === 0) return '';
  // приоритет ссылок, содержащих /cinema/movie/
  urls.sort((a, b) => {
    const pa = a.includes('/cinema/movie/') ? 0 : 1;
    const pb = b.includes('/cinema/movie/') ? 0 : 1;
    if (pa !== pb) return pa - pb;
    return a.length - b.length; // короче — вероятно релевантнее
  });
  return urls[0];
}

async function ensureDir(dir) {
  await fs.promises.mkdir(dir, { recursive: true });
}
function safeBaseName(name) {
  return String(name).replace(/[^a-z0-9._-]/gi, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
}
function getExtFromUrl(u) {
  const m = String(u).match(/\.([a-zA-Z0-9]+)(?:[?#]|$)/);
  return m ? m[12].toLowerCase() : '';
}
function extFromContentType(ct, fallback = 'jpg') {
  if (!ct) return fallback;
  const t = ct.split(';').trim().toLowerCase();
  if (t === 'image/jpeg' || t === 'image/jpg') return 'jpg';
  if (t === 'image/png') return 'png';
  if (t === 'image/webp') return 'webp';
  if (t === 'image/gif') return 'gif';
  if (t === 'image/svg+xml') return 'svg';
  if (t === 'image/avif') return 'avif';
  return fallback;
}
function pickImageUrlFromImg($, img, baseUrl) {
  const $img = $(img);
  const candidates = [
    $img.attr('src'),
    $img.attr('data-src'),
    $img.attr('data-original'),
    $img.attr('data-lazy'),
  ].filter(Boolean);
  const srcset = $img.attr('srcset');
  if (srcset) {
    const first = String(srcset).split(',').trim().split(/\s+/);
    if (first) candidates.unshift(first);
  }
  const url = candidates.find(Boolean) || '';
  return url ? toAbsUrl(url, baseUrl) : null;
}
async function downloadImage(imageUrl, destDir, fileBase, referer, cookieString) {
  await ensureDir(destDir);
  let ext = getExtFromUrl(imageUrl);
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Crawlee/cheerio',
    'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
    'Referer': referer || new URL(CINEMA_URL).origin,
    'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
  };
  if (cookieString) headers['Cookie'] = cookieString;
  let res, lastErr;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      res = await fetch(imageUrl, { headers, redirect: 'follow' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      break;
    } catch (e) {
      lastErr = e;
      const backoffMs = 300 * attempt;
      log.warning(`Попытка ${attempt} для ${imageUrl} не удалась: ${e.message}. Жду ${backoffMs} мс`);
      await new Promise(r => setTimeout(r, backoffMs));
    }
  }
  if (!res || !res.ok) throw new Error(`Не удалось скачать: ${imageUrl} (${lastErr?.message || 'нет ответа'})`);
  if (!ext) {
    const ct = res.headers.get('content-type') || '';
    ext = extFromContentType(ct, 'jpg');
  }
  const base = safeBaseName(fileBase);
  const filename = `${base}.${ext}`;
  const filepath = path.join(destDir, filename);
  try { await fs.promises.access(filepath, fs.constants.F_OK); log.info(`Файл уже существует, пропускаю: ${filename}`); return filepath; } catch {}
  const buf = Buffer.from(await res.arrayBuffer());
  await fs.promises.writeFile(filepath, buf);
  log.info(`Сохранено изображение: ${filename}`);
  return filepath;
}

// Описание из карточки (без изменений)
function extractDescription(movie) {
  const candidates = [
    '.scheduleMovie__item-content .desc',
    '.scheduleMovie__item-content .description',
    '.scheduleMovie__item-content .text',
    '.scheduleMovie__item-content p',
  ];
  for (const sel of candidates) {
    const t = movie.find(sel).first().text().trim();
    if (t) return t;
  }
  return '';
}

// Выбор ссылки на страницу фильма (а не расписания) — возвращает ОДИН URL
function extractMoviePageUrl($, movie, baseUrl, dd) {
  const candidates = [
    movie.find('a.movie__item-cover').attr('href'),                    // основной вариант
    movie.find('.scheduleMovie__item-poster a').attr('href'),          // альтернатива
    movie.find('.scheduleMovie__item-content .title a').attr('href'),  // ссылка в заголовке
    dd?.movieUrl, dd?.url, dd?.href, dd?.link,                         // из JSON, если есть
  ].filter(Boolean);
  return pickBestMovieUrl(candidates, baseUrl);
}

// ---------- парсинг «Завтра» ----------
function extractTomorrowSessions($, baseUrl) {
  const tomorrowItem = $('.module.schedule .header .content-list .item.enable')
    .filter((_, el) => $(el).find('.label-h1').first().text().trim().toLowerCase() === 'завтра')
    .first();

  let tomorrowDate = tomorrowItem.attr('data-date');
  if (!tomorrowDate) {
    const items = $('.module.schedule .header .content-list .item');
    const todayIdx = items.toArray().findIndex(el =>
      $(el).find('.label-h1').first().text().trim().toLowerCase() === 'сегодня'
    );
    if (todayIdx >= 0 && todayIdx + 1 < items.length) {
      tomorrowDate = $(items.get(todayIdx + 1)).attr('data-date');
    }
  }
  if (!tomorrowDate) return [];

  const dayBlock = $(`.item.day[data-date="${tomorrowDate}"]`);
  if (!dayBlock.length) return [];

  const sessions = [];
  dayBlock.find('.scheduleList .scheduleMovie__item').each((_, movieEl) => {
    const movie = $(movieEl);

    const baseTitle = movie.find('.scheduleMovie__item-content .title h3').first().text().trim() || '';
    const rawText = movie.find('.scheduleMovie__item-content').first().text() || '';
    const ageMatch = rawText.match(/\b([0-9]{1,2}\s*\+)\b/);
    const age = ageMatch ? ageMatch[12].replace(/\s+/g, '') : null;
    const abbtitle =
      movie.find('.scheduleMovie__item-content .title .abbr, .scheduleMovie__item-content .title abbr').first().text().trim()
      || baseTitle;
    const description = extractDescription(movie);

    let coverUrl =
      pickImageUrlFromImg($, movie.find('.scheduleMovie__item-poster img').first(), baseUrl) ||
      pickImageUrlFromImg($, movie.find('.scheduleMovie__item-hposter img').first(), baseUrl) ||
      null;

    movie.find('.seances .format .content-list a.btn.btn__time.sale, .seances .format .content-list a.btn.btn__time').each((__, aEl) => {
      const a = $(aEl);
      const dataData = a.attr('data-data') || '{}';
      let dd = {};
      try { dd = JSON.parse(dataData); } catch {}

      const tsSec = Number(dd?.timestamp || 0);
      if (!baseTitle || !tsSec) return;

      const dateStart = new Date(tsSec * 1000).toISOString();
      const title = age ? `${baseTitle}, ${age}` : baseTitle;
      const site = extractMoviePageUrl($, movie, baseUrl, dd); // теперь ровно один чистый URL
      const slug = slugify(`${baseTitle}-${tsSec}`);

      sessions.push({
        title,
        abbtitle,
        slug,
        dateStart,
        site,           // чистый абсолютный URL на страницу фильма
        description,    // как раньше
        _coverUrl: coverUrl, // служебно для скачивания
      });
    });
  });
  return sessions;
}

// ---------- основной поток ----------
async function main() {
  const sessions = [];

  const crawler = new CheerioCrawler({
    useSessionPool: true,
    persistCookiesPerSession: true,
    requestHandlerTimeoutSecs: 60,
    maxRequestRetries: 2,
    async requestHandler({ request, $, log, session }) {
      log.info(`Загружаю: ${request.url}`);
      const items = extractTomorrowSessions($, request.url);
      log.info(`Сеансов на завтра: ${items.length}`);

      // Скачивание картинок — не меняем, как просили
      const referer = request.url;
      const tasks = [];
      for (const s of items) {
        const coverUrl = s._coverUrl;
        if (!coverUrl) continue;
        const hash = crypto.createHash('md5').update(coverUrl).digest('hex').slice(0, 8);
        const fileBase = `${s.slug}-${hash}`;
        const cookieString =
          session?.getCookieString?.(coverUrl) || session?.getCookieString?.(request.url) || '';
        tasks.push(
          downloadImage(coverUrl, IMAGES_DIR, fileBase, referer, cookieString).catch((e) =>
            log.warning(`Не удалось скачать обложку: ${coverUrl} -> ${e.message}`)
          )
        );
      }
      await Promise.allSettled(tasks);

      // Очистка служебных полей
      for (const s of items) delete s._coverUrl;

      sessions.push(...items);
    },
  }, config);

  await crawler.run([CINEMA_URL]);
  console.log(JSON.stringify(sessions, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

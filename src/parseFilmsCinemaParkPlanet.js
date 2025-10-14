// index.js
// package.json -> { "type": "module" }
// npm i crawlee slugify dotenv

import dotenv from "dotenv";
import { CheerioCrawler, log, Configuration } from "crawlee";
import slugifyLib from "slugify";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Загружаем .env файл
dotenv.config({ path: path.resolve(__dirname, '../.env') });

// Источник расписания
const CINEMA_URL =
    process.env.CINEMA_URL || "https://kinoteatr.ru/raspisanie-kinoteatrov/perm/planeta/";
// Папка для изображений
const IMAGES_DIR = process.env.IMAGES_DIR || path.join(__dirname, "images");

// Strapi настройки
const STRAPI_URL = (process.env.STRAPI_URL || "http://127.0.0.1:1337").replace(
    /\/+$/,
    ""
);
const STRAPI_TOKEN = process.env.STRAPI_TOKEN || "";
const COLLECTION = "parties";
const STRAPI_CONTENT_UID = process.env.STRAPI_CONTENT_UID || "api::party.party";
const LOCALE = process.env.STRAPI_LOCALE || "";

// Жёстко требуемый place id
const FIXED_PLACE_ID = 10984;

// КОЛИЧЕСТВО ДНЕЙ ДЛЯ ПАРСИНГА (включая сегодня)
const DAYS_TO_PARSE = 10;

// Конфигурация Crawlee
const config = new Configuration({
    systemInfoV2: true,
    logLevel: "DEBUG"
});

// ---------- утилиты ----------
function slugify(input) {
    return slugifyLib(input, { lower: true, strict: true, trim: true });
}

function toAbsUrl(maybeUrl, base) {
    try {
        return new URL(maybeUrl, base).toString();
    } catch {
        if (maybeUrl?.startsWith?.("/")) return new URL(base).origin + maybeUrl;
        return maybeUrl || base;
    }
}

function isValidHttpUrl(u) {
    try {
        const url = new URL(u);
        return url.protocol === "http:" || url.protocol === "https:";
    } catch {
        return false;
    }
}

function getCookieStringSafe(session, url, fallbackUrl) {
    try {
        const pick = isValidHttpUrl(url)
            ? url
            : isValidHttpUrl(fallbackUrl)
            ? fallbackUrl
            : "";
        if (!pick) return "";
        return session?.getCookieString?.(pick) || "";
    } catch {
        return "";
    }
}

async function ensureDir(dir) {
    await fs.promises.mkdir(dir, { recursive: true });
}

function safeBaseName(name) {
    return String(name)
        .replace(/[^a-z0-9._-]/gi, "-")
        .replace(/-+/g, "-")
        .replace(/^-|-$/g, "");
}

function getExtFromUrl(u) {
    const s = typeof u === "string" ? u : u?.toString?.() ?? "";
    const m = s.match(/\.([a-zA-Z0-9]+)(?:[?#]|$)/);
    return m && typeof m[1] === "string" ? m[1].toLowerCase() : "";
}

function extFromContentType(ct, fallback = "jpg") {
    try {
        const base = String(ct ?? "")
            .split(";")[0]
            .trim()
            .toLowerCase();
        if (!base) return fallback;
        if (base === "image/jpeg" || base === "image/jpg") return "jpg";
        if (base === "image/png") return "png";
        if (base === "image/webp") return "webp";
        if (base === "image/gif") return "gif";
        if (base === "image/svg+xml") return "svg";
        if (base === "image/avif") return "avif";
        return fallback;
    } catch {
        return fallback;
    }
}

function mimeFromExt(ext) {
    const e = String(ext || "").toLowerCase();
    if (e === "jpg" || e === "jpeg") return "image/jpeg";
    if (e === "png") return "image/png";
    if (e === "webp") return "image/webp";
    if (e === "gif") return "image/gif";
    if (e === "svg") return "image/svg+xml";
    if (e === "avif") return "image/avif";
    return "application/octet-stream";
}

function pickImageUrlFromImg($, img, baseUrl) {
    const $img = $(img);
    const candidates = [
        $img.attr("src"),
        $img.attr("data-src"),
        $img.attr("data-original"),
        $img.attr("data-lazy"),
    ].filter(Boolean);
    const srcset = $img.attr("srcset");
    if (srcset) {
        const first = String(srcset).split(",")[0].trim().split(/\s+/)[0];
        if (first) candidates.unshift(first);
    }
    const url = candidates.find(Boolean) || "";
    return url ? toAbsUrl(url, baseUrl) : null;
}

// ---------- функции времени ----------
function formatTime(date) {
    return date.toLocaleTimeString("ru-RU", {
        hour: "2-digit",
        minute: "2-digit",
    });
}

function formatDate(date) {
    return date.toLocaleDateString("ru-RU", {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
    });
}

// Функция для создания ISO строки с часовым поясом +05:00 (Пермь)
function toISOString(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}+05:00`;
}

// Извлекаем дату из URL
function extractDateFromUrl(url) {
    try {
        const urlObj = new URL(url);
        const dateParam = urlObj.searchParams.get('date');
        if (dateParam) {
            const [year, month, day] = dateParam.split('-').map(n => parseInt(n, 10));
            if (!isNaN(year) && !isNaN(month) && !isNaN(day)) {
                return new Date(year, month - 1, day);
            }
        }
    } catch {}
    return new Date();
}

// ---------- группировка сеансов ----------
function groupSessionsByMovieAndDay(sessions, daysCount, pageDate) {
    // Фильтруем от даты страницы
    const start = new Date(pageDate.getFullYear(), pageDate.getMonth(), pageDate.getDate());
    const end = new Date(start.getTime() + daysCount * 24 * 60 * 60 * 1000);

    log.info(`Парсим сеансы на ${daysCount} дней с ${start.toDateString()} по ${end.toDateString()}`);

    const groups = new Map();

    for (const session of sessions) {
        const sessionStart = new Date(session.originalDate);
        if (sessionStart < start || sessionStart >= end) continue;
        const dayKey = sessionStart.toDateString();
        const movieKey = `${session.baseTitle}-${dayKey}`;
        if (!groups.has(movieKey)) groups.set(movieKey, []);
        groups.get(movieKey).push({ ...session, sessionStart });
    }

    const result = [];
    for (const [_, movieSessions] of groups) {
        movieSessions.sort((a, b) => a.sessionStart.getTime() - b.sessionStart.getTime());
        const earliest = movieSessions[0];
        
        // Для описания используем оригинальное время
        const allTimes = movieSessions.map((s) => {
            const date = formatDate(s.sessionStart);
            const time = formatTime(s.sessionStart);
            return `${date} в ${time}`;
        });
        
        const mainSession = {
            ...earliest,
            dateStart: earliest.dateStart, // Время уже в правильном формате
            // Slug с place_id для уникальности
            slug: slugify(`${FIXED_PLACE_ID}-${earliest.baseTitle}-${formatDate(earliest.sessionStart).replace(/\./g, "-")}`),
            allShowTimes: allTimes,
        };
        result.push(mainSession);
    }
    return result;
}

async function testConnection(url) {
    try {
        log.info(`Тестируем соединение с ${url}...`);
        const res = await fetch(url, {
            signal: AbortSignal.timeout(10000),
            headers: {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            }
        });
        log.info(`Соединение успешно. Status: ${res.status}`);
        return true;
    } catch (e) {
        log.error(`Ошибка соединения с ${url}: ${e.message}`);
        return false;
    }
}

async function downloadImage(imageUrl, destDir, fileBase, referer, cookieString) {
    await ensureDir(destDir);
    let ext = getExtFromUrl(imageUrl);
    const headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Crawlee/cheerio",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Referer": referer || new URL(CINEMA_URL).origin,
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    };
    if (cookieString) headers["Cookie"] = cookieString;

    let res, lastErr;
    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            res = await fetch(imageUrl, {
                headers,
                redirect: "follow",
                signal: AbortSignal.timeout(15000)
            });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            break;
        } catch (e) {
            lastErr = e;
            const backoffMs = 300 * attempt;
            log.warning(`Попытка ${attempt} для ${imageUrl} не удалась: ${e.message}. Жду ${backoffMs} мс`);
            await new Promise((r) => setTimeout(r, backoffMs));
        }
    }
    
    if (!res || !res.ok) {
        throw new Error(`Не удалось скачать: ${imageUrl} (${lastErr?.message || "нет ответа"})`);
    }
    
    if (!ext) {
        const ct = res.headers.get("content-type");
        ext = extFromContentType(ct, "jpg");
    }
    if (!ext) ext = "jpg";

    const base = safeBaseName(fileBase);
    const filename = `${base}.${ext}`;
    const filepath = path.join(destDir, filename);

    try {
        await fs.promises.access(filepath, fs.constants.F_OK);
        log.info(`Файл уже существует, пропускаю: ${filename}`);
        return filepath;
    } catch {}
    
    const buf = Buffer.from(await res.arrayBuffer());
    await fs.promises.writeFile(filepath, buf);
    log.info(`Сохранено изображение: ${filename}`);
    return filepath;
}

function decodeEntities(s) {
    return String(s)
        .replace(/&nbsp;/g, " ")
        .replace(/&amp;/g, "&")
        .replace(/&quot;/g, '"')
        .replace(/&#39;|&apos;/g, "'")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/&laquo;/g, "«")
        .replace(/&raquo;/g, "»")
        .replace(/\s{2,}/g, " ")
        .trim();
}

// Извлечение описания с фильма
function extractDescriptionFromMoviePage($) {
    const desc = $('p[itemprop="description"]').text().trim();
    if (desc) return decodeEntities(desc);
    const metaDesc = $('meta[property="og:description"]').attr('content');
    return metaDesc ? decodeEntities(metaDesc) : "";
}

async function fetchMovieDescription(movieUrl, cookieString, referer) {
    const headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Crawlee/cheerio",
        "Accept": "text/html,application/xhtml+xml",
        "Referer": referer || new URL(CINEMA_URL).origin,
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    };
    if (cookieString) headers["Cookie"] = cookieString;
    
    try {
        const res = await fetch(movieUrl, {
            headers,
            redirect: "follow",
            signal: AbortSignal.timeout(10000)
        });
        if (!res.ok) return "";
        const html = await res.text();
        
        const cheerio = await import('cheerio');
        const $ = cheerio.load(html);
        return extractDescriptionFromMoviePage($);
    } catch (e) {
        log.warning(`Ошибка получения описания с ${movieUrl}: ${e.message}`);
        return "";
    }
}

function describeFetchError(e) {
    if (!e) return "unknown";
    const cause = e.cause || e;
    const code = cause?.code || cause?.errno || "";
    const addr = cause?.address || "";
    const port = cause?.port || "";
    return `${e.message} (${code}${addr ? ` ${addr}` : ""}${port ? `:${port}` : ""})`;
}

// Strapi клиент
class StrapiClient {
    constructor(base, token) {
        this.base = base.replace(/\/+$/, "");
        this.token = token;
    }

    headers(json = true) {
        const h = {};
        if (json) h["Content-Type"] = "application/json";
        if (this.token) h["Authorization"] = `Bearer ${this.token}`;
        return h;
    }

    async get(pathname) {
        const res = await fetch(`${this.base}${pathname}`, {
            method: "GET",
            headers: this.headers(false),
            signal: AbortSignal.timeout(30000)
        });
        if (!res.ok) throw new Error(`GET ${pathname} -> ${res.status}`);
        return res.json();
    }

    async post(pathname, body) {
        const res = await fetch(`${this.base}${pathname}`, {
            method: "POST",
            headers: this.headers(true),
            body: JSON.stringify(body),
            signal: AbortSignal.timeout(30000)
        });
        if (!res.ok) {
            const t = await res.text().catch(() => "");
            throw new Error(`POST ${pathname} -> ${res.status}\n${t}`);
        }
        return res.json();
    }

    async put(pathname, body) {
        const res = await fetch(`${this.base}${pathname}`, {
            method: "PUT",
            headers: this.headers(true),
            body: JSON.stringify(body),
            signal: AbortSignal.timeout(30000)
        });
        if (!res.ok) {
            const t = await res.text().catch(() => "");
            throw new Error(`PUT ${pathname} -> ${res.status}\n${t}`);
        }
        return res.json();
    }

    qLocale(params = {}) {
        const q = new URLSearchParams(params);
        if (LOCALE) q.set("locale", LOCALE);
        const s = q.toString();
        return s ? `?${s}` : "";
    }

    // ИСПРАВЛЕНО: поиск по slug + dateStart + place (все состояния: live + preview)
    async findPartyBySlug(slug, dateStart, placeId) {
        const params = new URLSearchParams();
        params.set("filters[slug][$eq]", slug);
        params.set("filters[dateStart][$eq]", dateStart);
        params.set("filters[place][id][$eq]", String(placeId));
        params.set("pagination[pageSize]", "1");
        // Убираем publicationState - ищем все (live + preview)
        if (LOCALE) params.set("locale", LOCALE);
        
        try {
            const data = await this.get(`/api/${COLLECTION}?${params.toString()}`);
            if (data?.data?.length) {
                const row = data.data[0];
                log.info(`Найдена существующая запись: ${slug} -> ID ${row.id}`);
                return row && Number.isFinite(row.id) ? row : null;
            }
        } catch (e) {
            log.warning(`Ошибка поиска по фильтрам для ${slug}: ${e.message}`);
        }
        
        log.info(`Запись не найдена: ${slug} - будет создана новая`);
        return null;
    }

    async uploadAndLinkFile(localPath, caption, alt, refId, field = "cover") {
        const baseName = path.basename(localPath);
        const ext = (baseName.split(".").pop() || "").toLowerCase();
        const mime = mimeFromExt(ext);
        const buf = await fs.promises.readFile(localPath);
        const file = new Blob([buf], { type: mime });

        const form = new FormData();
        form.append("files", file, baseName);
        form.append("ref", STRAPI_CONTENT_UID);
        form.append("refId", String(refId));
        form.append("field", field);
        form.append(
            "fileInfo",
            JSON.stringify({
                caption: caption || baseName,
                alternativeText: alt || baseName,
            })
        );

        const res = await fetch(`${this.base}/api/upload`, {
            method: "POST",
            headers: this.token ? { Authorization: `Bearer ${this.token}` } : {},
            body: form,
            signal: AbortSignal.timeout(60000)
        });

        if (!res.ok) {
            const t = await res.text().catch(() => "");
            throw new Error(`Upload failed: ${res.status}\n${t}`);
        }

        const files = await res.json();
        if (!Array.isArray(files) || !files.length) {
            throw new Error("Upload returned empty array");
        }
        return files[0];
    }

    // Обработка 404 и конфликтов slug (с новым поиском)
    async upsertParty(p) {
        let existing = null;
        try {
            existing = await this.findPartyBySlug(p.slug, p.dateStart, FIXED_PLACE_ID);
        } catch (e) {
            log.warning(`Ошибка поиска записи ${p.slug}: ${e.message}`);
        }
        
        let finalDescription = "";

        if (p.allShowTimes && p.allShowTimes.length > 1) {
            const showtimesText = "**Расписание сеансов:**\n" +
                p.allShowTimes.map(time => `• ${time}`).join("\n");
            finalDescription += showtimesText;
        }

        if (p.description && p.description.trim()) {
            if (finalDescription) {
                finalDescription += "\n\n" + p.description.trim();
            } else {
                finalDescription = p.description.trim();
            }
        }

        const baseData = {
            title: p.title,
            abbTitle: p.abbTitle,
            slug: p.slug,
            dateStart: p.dateStart,
            site: p.site,
            tel: p.tel || "",
            categories: [28],
            forCities: [22],
            place: FIXED_PLACE_ID,
            discount: "15%",
            discountRule: `Скидка на покупку билетов по промокоду **10086** по ссылке выше. Для получения скидки нужно ввести код в поле "Промокод"`,
            ...(LOCALE ? { locale: LOCALE } : {}),
        };

        if (finalDescription.trim()) {
            baseData.description = finalDescription.trim();
        }

        if (existing && Number.isFinite(existing.id)) {
            try {
                const qs = this.qLocale();
                const res = await this.put(
                    `/api/${COLLECTION}/${existing.id}${qs}`,
                    { data: baseData }
                );
                const id = res?.data?.id ?? res?.id ?? existing.id;
                log.info(`Обновлена запись ID ${id} для slug ${p.slug}`);
                return { id, data: res?.data || res };
            } catch (e) {
                // Если 404 - запись удалена, но комбо slug+date+place занято
                if (e.message.includes('404')) {
                    log.warning(`Запись ${existing.id} не найдена (404), но комбо ${p.slug}+${p.dateStart} занято. Пропускаем.`);
                    return { id: null, data: null };
                }
                throw e;
            }
        } else {
            try {
                const res = await this.post(`/api/${COLLECTION}`, {
                    data: baseData,
                });
                const id = res?.data?.id ?? res?.id;
                log.info(`Создана новая запись ID ${id} для slug ${p.slug}`);
                return { id, data: res?.data || res };
            } catch (e) {
                // Если slug/date/place уникально занято
                if (e.message.includes('unique') || e.message.includes('400')) {
                    log.warning(`Комбо ${p.slug}+${p.dateStart} уже существует. Пропускаем.`);
                    return { id: null, data: null };
                }
                throw e;
            }
        }
    }
}

// ---------- парсинг расписания ----------
function extractAllSessions($, baseUrl, pageDate) {
    const sessions = [];
    
    $(".shedule_movie.bordered").each((_, movieEl) => {
        const movie = $(movieEl);
        
        // Название фильма
        const baseTitle = movie.find(".movie_card_header.title").first().text().trim() || "";
        if (!baseTitle) return;
        
        // Возрастной рейтинг
        const ratingText = movie.find(".movie_card_raiting.sub_title").text() || "";
        const ageMatch = ratingText.match(/\b([0-9]{1,2}\s*\+)\b/);
        const age = ageMatch ? ageMatch[1].replace(/\s+/g, "") : null;
        const title = age ? `${baseTitle}, ${age}` : baseTitle;
        const abbtitle = title;
        
        // Постер
        let coverUrl = pickImageUrlFromImg($, movie.find(".shedule_movie_img").first(), baseUrl) || null;
        
        // Ссылка на фильм
        let site = movie.find("a.gtm-ec-list-item-movie").attr("href") || "";
        if (site && !isValidHttpUrl(site)) site = toAbsUrl(site, baseUrl);
        
        // Сеансы
        movie.find(".shedule_movie_sessions a.buy_seance").each((__, seanceEl) => {
            const seance = $(seanceEl);
            
            const timeText = seance.find(".shedule_session_time").text().trim();
            if (!timeText) return;
            
            const [hours, minutes] = timeText.split(":").map(n => parseInt(n, 10));
            if (isNaN(hours) || isNaN(minutes)) return;
            
            // Оригинальная дата сеанса
            const sessionDate = new Date(
                pageDate.getFullYear(),
                pageDate.getMonth(),
                pageDate.getDate(),
                hours,
                minutes,
                0
            );
            
            sessions.push({
                title,
                abbtitle,
                baseTitle,
                originalDate: sessionDate, // Для описания
                dateStart: toISOString(sessionDate), // Для базы
                dateEnd: null,
                site,
                description: "",
                _coverUrl: coverUrl,
            });
        });
    });
    
    return sessions;
}

// ---------- основной поток ----------
async function main() {
    log.info(`Strapi base: ${STRAPI_URL}`);
    log.info(`STRAPI_TOKEN length: ${(STRAPI_TOKEN || "").length}`);
    log.info(`STRAPI_CONTENT_UID: ${STRAPI_CONTENT_UID}`);
    log.info(`LOCALE: ${LOCALE || "(default)"}`);
    log.info(`Парсим сеансы на ${DAYS_TO_PARSE} дней`);

    // Формируем URLs с параметром даты
    const baseUrl = CINEMA_URL.split('?')[0];
    const urlsToParse = [];
    const now = new Date();
    for (let i = 0; i < DAYS_TO_PARSE; i++) {
        const d = new Date(now.getTime() + i * 24 * 60 * 60 * 1000);
        const dateStr = d.toISOString().split('T')[0];
        urlsToParse.push(`${baseUrl}?date=${dateStr}`);
    }

    const connectionOk = await testConnection(urlsToParse[0]);
    if (!connectionOk) {
        log.error("Не удается подключиться к сайту кинотеатра. Завершаю работу.");
        process.exit(1);
    }

    const sessions = [];
    const descCache = new Map();
    const strapi = new StrapiClient(STRAPI_URL, STRAPI_TOKEN);
    
    if (!STRAPI_TOKEN) {
        log.warning("STRAPI_TOKEN не задан — Strapi может отклонять запросы");
    }

    const crawler = new CheerioCrawler(
        {
            useSessionPool: true,
            persistCookiesPerSession: true,
            requestHandlerTimeoutSecs: 120,
            navigationTimeoutSecs: 60,
            maxRequestRetries: 3,
            maxConcurrency: 1,
            minConcurrency: 1,
            
            async requestHandler({ request, $, log, session }) {
                log.info(`Загружаю: ${request.url}`);
                
                // ИЗВЛЕКАЕМ ДАТУ ИЗ URL
                const pageDate = extractDateFromUrl(request.url);
                log.info(`Дата страницы: ${pageDate.toDateString()}`);
                
                const rawItems = extractAllSessions($, request.url, pageDate);
                log.info(`Найдено сеансов: ${rawItems.length}`);

                // Передаём pageDate в groupSessionsByMovieAndDay
                const groupedItems = groupSessionsByMovieAndDay(rawItems, DAYS_TO_PARSE, pageDate);
                log.info(`После группировки и фильтрации: ${groupedItems.length} записей`);

                const referer = request.url;
                const tasks = [];
                const coverPathByUrl = new Map();

                // Загружаем описания фильмов
                const uniqueSites = Array.from(new Set(groupedItems.map((s) => s.site))).filter(isValidHttpUrl);
                for (const movieUrl of uniqueSites) {
                    if (descCache.has(movieUrl)) continue;
                    const cookieString = getCookieStringSafe(session, movieUrl, referer);
                    tasks.push(
                        (async () => {
                            try {
                                const desc = await fetchMovieDescription(movieUrl, cookieString, referer);
                                descCache.set(movieUrl, desc || "");
                            } catch (e) {
                                log.warning(`Ошибка получения описания для ${movieUrl}: ${e.message}`);
                                descCache.set(movieUrl, "");
                            }
                        })()
                    );
                }

                // Загружаем изображения
                for (const s of groupedItems) {
                    const coverUrl = s._coverUrl;
                    if (!coverUrl) continue;
                    const hash = crypto.createHash("md5").update(coverUrl).digest("hex").slice(0, 8);
                    const fileBase = `${s.slug}-${hash}`;
                    const cookieString = getCookieStringSafe(session, coverUrl, referer);
                    tasks.push(
                        (async () => {
                            try {
                                const p = await downloadImage(coverUrl, IMAGES_DIR, fileBase, referer, cookieString);
                                coverPathByUrl.set(coverUrl, p);
                            } catch (e) {
                                log.warning(`Не удалось скачать обложку: ${coverUrl} -> ${e.message}`);
                            }
                        })()
                    );
                }

                await Promise.allSettled(tasks);

                // Добавляем описания к фильмам
                for (const s of groupedItems) {
                    if (!s.description && isValidHttpUrl(s.site) && descCache.has(s.site)) {
                        s.description = descCache.get(s.site) || "";
                    }
                }

                // Создаем/обновляем записи в Strapi (ИСПРАВЛЕНО: передаём dateStart и place в поиск)
                for (const s of groupedItems) {
                    try {
                        const party = {
                            title: s.title,
                            abbTitle: s.abbtitle,
                            slug: s.slug,
                            dateStart: s.dateStart,
                            site: s.site,
                            tel: "",
                            description: s.description || "",
                            allShowTimes: s.allShowTimes,
                        };
                        const saved = await strapi.upsertParty(party);
                        const partyId = saved?.id;
                        
                        // Проверка на null
                        if (!partyId) {
                            log.warning(`Не удалось создать/обновить запись для ${s.title} (${s.slug})`);
                            continue;
                        }

                        // Привязываем обложку
                        if (s._coverUrl) {
                            const localPath = coverPathByUrl.get(s._coverUrl);
                            if (localPath) {
                                try {
                                    await strapi.uploadAndLinkFile(localPath, s._coverUrl, s.title, partyId, "cover");
                                    log.info(`Cover успешно привязан к party=${partyId}`);
                                } catch (e) {
                                    log.warning(`Не удалось привязать cover: ${e.message}`);
                                }
                            }
                        }
                    } catch (e) {
                        log.warning(`Strapi upsert failed for "${s.title}" (${s.slug}): ${describeFetchError(e)}`);
                    }
                }

                // Очищаем временные данные
                for (const s of groupedItems) {
                    delete s._coverUrl;
                    delete s.originalDate;
                }
                sessions.push(...groupedItems);
            },
            async failedRequestHandler({ request, error }) {
                log.error(`Запрос ${request.url} не удался: ${error.message}`);
            }
        },
        config
    );

    log.info("Запускаем краулер...");
    await crawler.run(urlsToParse);
    log.info(`Краулер завершил работу. Обработано ${sessions.length} записей.`);
}

main().catch((e) => {
    console.error("Критическая ошибка:", e);
    process.exit(1);
});

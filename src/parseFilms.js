// index.js
// package.json -> { "type": "module" }
// npm i crawlee slugify dotenv
// Windows: $env:CRAWLEE_SYSTEM_INFO_V2='1'; node index.js

import "dotenv/config";
import { CheerioCrawler, log, Configuration } from "crawlee";
import slugifyLib from "slugify";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Источник расписания
const CINEMA_URL =
    process.env.CINEMA_URL || "https://almazcinema.com/ijv/cinema/53/schedule/";
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
const FIXED_PLACE_ID = 10611;

// КОЛИЧЕСТВО ДНЕЙ ДЛЯ ПАРСИНГА (включая сегодня)
// 1 = только сегодня, 2 = сегодня и завтра, 7 = неделя
const DAYS_TO_PARSE = 7;

// Конфигурация Crawlee
const config = new Configuration({ systemInfoV2: true });

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
function stripMarkdownUrl(s) {
    const str = String(s || "").trim();
    const md = str.match(/\((https?:\/\/[^\s)]+)\)/);
    if (md?.[1]) return md[1];
    const link = str.match(/https?:\/\/[^\s,]+/);
    return link ? link[0] : str.replace(/^[\s,]+|[\s,]+$/g, "");
}
function normalizeOneUrl(u, base) {
    const raw = stripMarkdownUrl(u);
    if (!raw) return "";
    try {
        const abs = new URL(raw, base);
        const pathname = abs.pathname.endsWith("/")
            ? abs.pathname
            : abs.pathname + "/";
        return `${abs.origin}${pathname}`;
    } catch {
        return "";
    }
}
function pickBestMovieUrl(cands, base) {
    const seen = new Set();
    const urls = [];
    for (const c of cands) {
        const v = normalizeOneUrl(c, base);
        if (v && !seen.has(v)) {
            seen.add(v);
            urls.push(v);
        }
    }
    if (!urls.length) return "";
    urls.sort((a, b) => {
        const pa = a.includes("/cinema/movie/") ? 0 : 1;
        const pb = b.includes("/cinema/movie/") ? 0 : 1;
        if (pa !== pb) return pa - pb;
        return a.length - b.length;
    });
    return urls[0];
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
function logResponseHeaders(res, url) {
    try {
        const ct = res.headers.get("content-type");
        const cl = res.headers.get("content-length");
        const server = res.headers.get("server");
        log.info(
            `HEADERS for ${url}: content-type=${ct ?? "null"} content-length=${cl ?? "null"} server=${server ?? "null"}`
        );
    } catch {}
}

// ---------- функции времени ----------
function formatTime(date) {
    return date.toLocaleTimeString("ru-RU", {
        hour: "2-digit",
        minute: "2-digit",
        timeZone: "Europe/Moscow",
    });
}

function formatDate(date) {
    return date.toLocaleDateString("ru-RU", {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        timeZone: "Europe/Moscow",
    });
}

// ---------- группировка сеансов ----------
function groupSessionsByMovieAndDay(sessions, daysCount) {
    const now = new Date();
    const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const endDate = new Date(todayStart.getTime() + daysCount * 24 * 60 * 60 * 1000);

    log.info(`Парсим сеансы на ${daysCount} дней с ${todayStart.toDateString()} по ${endDate.toDateString()}`);

    const groups = new Map();

    for (const session of sessions) {
        const sessionStart = new Date(session.dateStart);
        if (sessionStart < todayStart || sessionStart >= endDate) {
            continue;
        }
        const dayKey = sessionStart.toDateString();
        const movieKey = `${session.baseTitle}-${dayKey}`;
        if (!groups.has(movieKey)) {
            groups.set(movieKey, []);
        }
        groups.get(movieKey).push({ ...session, sessionStart });
    }

    const result = [];
    for (const [_, movieSessions] of groups) {
        movieSessions.sort((a, b) => a.sessionStart.getTime() - b.sessionStart.getTime());
        const earliest = movieSessions[0];
        const allTimes = movieSessions.map((s) => {
            const date = formatDate(s.sessionStart);
            const time = formatTime(s.sessionStart);
            return `${date} в ${time}`;
        });
        const mainSession = {
            ...earliest,
            dateStart: earliest.sessionStart.toISOString(),
            slug: slugify(`${earliest.baseTitle}-${formatDate(earliest.sessionStart).replace(/\./g, "-")}`),
            allShowTimes: allTimes,
        };
        result.push(mainSession);
    }

    return result;
}

async function downloadImage(imageUrl, destDir, fileBase, referer, cookieString) {
    await ensureDir(destDir);
    let ext = getExtFromUrl(imageUrl);
    const headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Crawlee/cheerio",
        Accept: "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        Referer: referer || new URL(CINEMA_URL).origin,
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    };
    if (cookieString) headers["Cookie"] = cookieString;

    let res, lastErr;
    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            res = await fetch(imageUrl, { headers, redirect: "follow" });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            break;
        } catch (e) {
            lastErr = e;
            const backoffMs = 300 * attempt;
            log.warning(`Попытка ${attempt} для ${imageUrl} не удалась: ${e.message}. Жду ${backoffMs} мс`);
            await new Promise((r) => setTimeout(r, backoffMs));
        }
    }
    if (!res || !res.ok) throw new Error(`Не удалось скачать: ${imageUrl} (${lastErr?.message || "нет ответа"})`);
    logResponseHeaders(res, imageUrl);
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
        .replace(/\s{2,}/g, " ")
        .trim();
}
function extractDescriptionFromHtml(html) {
    const tryMatch = (re) => {
        const m = html.match(re);
        if (m?.[1]) return decodeEntities(m[1].replace(/<[^>]+>/g, " "));
        return "";
    };
    let txt = tryMatch(/<div[^>]*class=["'][^"']*\bdescription\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/i);
    if (txt) return txt;
    txt = tryMatch(/<div[^>]*class=["'][^"']*\b(movie[-_ ]?desc|synopsis|summary|about)\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/i);
    if (txt) return txt;
    txt = tryMatch(/<div[^>]*itemprop=["']description["'][^>]*>([\s\S]*?)<\/div>/i);
    if (txt) return txt;
    const m1 = html.match(/<meta\s+name=["']description["']\s+content=["']([^"']+)["'][^>]*>/i);
    if (m1?.[1]) return decodeEntities(m1[1]);
    const m2 = html.match(/<meta\s+property=["']og:description["']\s+content=["']([^"']+)["'][^>]*>/i);
    if (m2?.[1]) return decodeEntities(m2[1]);
    return "";
}
async function fetchMovieDescription(movieUrl, cookieString, referer) {
    const headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Crawlee/cheerio",
        Accept: "text/html,application/xhtml+xml",
        Referer: referer || new URL(CINEMA_URL).origin,
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    };
    if (cookieString) headers["Cookie"] = cookieString;
    const res = await fetch(movieUrl, { headers, redirect: "follow" });
    if (!res.ok) return "";
    const html = await res.text();
    return extractDescriptionFromHtml(html);
}

function extractMoviePageUrl($, movie, baseUrl, dd) {
    const candidates = [
        movie.find("a.movie__item-cover").attr("href"),
        movie.find(".scheduleMovie__item-poster a").attr("href"),
        movie.find(".scheduleMovie__item-content .title a").attr("href"),
        dd?.movieUrl,
        dd?.url,
        dd?.href,
        dd?.link,
    ].filter(Boolean);
    return pickBestMovieUrl(candidates, baseUrl);
}

function describeFetchError(e) {
    if (!e) return "unknown";
    const cause = e.cause || e;
    const code = cause?.code || cause?.errno || "";
    const addr = cause?.address || "";
    const port = cause?.port || "";
    return `${e.message} (${code}${addr ? ` ${addr}` : ""}${port ? `:${port}` : ""})`;
}

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
        });
        if (!res.ok) throw new Error(`GET ${pathname} -> ${res.status}`);
        return res.json();
    }
    async post(pathname, body) {
        const res = await fetch(`${this.base}${pathname}`, {
            method: "POST",
            headers: this.headers(true),
            body: JSON.stringify(body),
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

    async findPartyBySlug(slug) {
        const params = new URLSearchParams();
        params.set("filters[slug][$eq]", slug);
        params.set("pagination[pageSize]", "1");
        params.set("publicationState", "preview");
        if (LOCALE) params.set("locale", LOCALE);
        const data = await this.get(`/api/${COLLECTION}?${params.toString()}`);
        if (data?.data?.length) {
            const row = data.data[0];
            return row && Number.isFinite(row.id) ? row : null;
        }
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
        });

        if (!res.ok) {
            const t = await res.text().catch(() => "");
            throw new Error(`Upload failed: ${res.status}\n${t}`);
        }

        const files = await res.json();
        if (!Array.isArray(files) || !files.length) throw new Error("Upload returned empty array");
        return files[0];
    }

    async upsertParty(p) {
        const existing = await this.findPartyBySlug(p.slug);
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
            forRegions: [4],
            place: FIXED_PLACE_ID,
            discount: "20%",
            discountRule: `Скидка на взрослый билет при покупке билета по ссылке выше по промокоду **99000006**\n**Для покупки билета**:\n- Нужно пройти регистрацию на сайте Алмаз Синема\n- Промокод вводить в поле "БОНУСЫ"\nВозможна покупка нескольких взрослых билетов.`,
            ...(LOCALE ? { locale: LOCALE } : {}),
        };

        if (finalDescription.trim()) {
            baseData.description = finalDescription.trim();
        }

        if (existing && Number.isFinite(existing.id)) {
            const qs = this.qLocale();
            const res = await this.put(
                `/api/${COLLECTION}/${existing.id}${qs}`,
                { data: baseData }
            );
            const id = res?.data?.id ?? res?.id ?? existing.id;
            return { id, data: res?.data || res };
        } else {
            const res = await this.post(`/api/${COLLECTION}`, {
                data: baseData,
            });
            const id = res?.data?.id ?? res?.id;
            return { id, data: res?.data || res };
        }
    }
}

// ---------- парсинг расписания ----------
function extractAllSessions($, baseUrl) {
    const sessions = [];
    $(".item.day").each((_, dayEl) => {
        const dayBlock = $(dayEl);
        dayBlock
            .find(".scheduleList .scheduleMovie__item")
            .each((__, movieEl) => {
                const movie = $(movieEl);
                const baseTitle =
                    movie
                        .find(".scheduleMovie__item-content .title h3")
                        .first()
                        .text()
                        .trim() || "";
                const rawText =
                    movie.find(".scheduleMovie__item-content").first().text() ||
                    "";
                const ageMatch = rawText.match(/\b([0-9]{1,2}\s*\+)\b/);
                const age = ageMatch ? ageMatch[1].replace(/\s+/g, "") : null;
                const abbtitle =
                    movie
                        .find(
                            ".scheduleMovie__item-content .title .abbr, .scheduleMovie__item-content .title abbr"
                        )
                        .first()
                        .text()
                        .trim() || baseTitle;

                let coverUrl =
                    pickImageUrlFromImg(
                        $,
                        movie.find(".scheduleMovie__item-poster img").first(),
                        baseUrl
                    ) ||
                    pickImageUrlFromImg(
                        $,
                        movie.find(".scheduleMovie__item-hposter img").first(),
                        baseUrl
                    ) ||
                    null;

                movie
                    .find(
                        ".seances .format .content-list a.btn.btn__time.sale, .seances .format .content-list a.btn.btn__time"
                    )
                    .each((___, aEl) => {
                        const a = $(aEl);
                        const dataData = a.attr("data-data") || "{}";
                        let dd = {};
                        try {
                            dd = JSON.parse(dataData);
                        } catch {}
                        const tsSec = Number(dd?.timestamp || 0);
                        const lengthMin = Number(
                            dd?.length || (dd?.duration?.release ?? 0)
                        );
                        if (!baseTitle || !tsSec) return;

                        // Смещаем дату начала на -1 час
                        const dateStart = new Date(tsSec * 1000 - 60 * 60 * 1000).toISOString();
                        const dateEnd =
                            lengthMin > 0
                                ? new Date(tsSec * 1000 - 60 * 60 * 1000 + lengthMin * 60000).toISOString()
                                : null;

                        const title = age ? `${baseTitle}, ${age}` : baseTitle;

                        let siteRaw = extractMoviePageUrl(
                            $,
                            movie,
                            baseUrl,
                            dd
                        );
                        let site = String(
                            Array.isArray(siteRaw) ? siteRaw[0] : siteRaw
                        ).trim();
                        site = site.replace(/^"+|"+$/g, "");
                        if (!isValidHttpUrl(site))
                            site = normalizeOneUrl(site, baseUrl);

                        sessions.push({
                            title,
                            abbtitle,
                            baseTitle,
                            dateStart,
                            dateEnd,
                            site,
                            description: "",
                            _coverUrl: coverUrl,
                        });
                    });
            });
    });
    return sessions;
}

// ---------- основной поток ----------
async function main() {
    log.info(`Strapi base: ${STRAPI_URL}`);
    log.info(`STRAPI_TOKEN length: ${(process.env.STRAPI_TOKEN || "").length}`);
    log.info(`STRAPI_CONTENT_UID: ${STRAPI_CONTENT_UID}`);
    log.info(`LOCALE: ${LOCALE || "(default)"}`);
    log.info(`Парсим сеансы на ${DAYS_TO_PARSE} дней`);

    const sessions = [];
    const descCache = new Map();
    const strapi = new StrapiClient(STRAPI_URL, STRAPI_TOKEN);
    if (!STRAPI_TOKEN) log.warning("STRAPI_TOKEN не задан — Strapi может отклонять запросы");

    const crawler = new CheerioCrawler(
        {
            useSessionPool: true,
            persistCookiesPerSession: true,
            requestHandlerTimeoutSecs: 60,
            maxRequestRetries: 2,
            async requestHandler({ request, $, log, session }) {
                log.info(`Загружаю: ${request.url}`);
                const rawItems = extractAllSessions($, request.url);
                log.info(`Найдено сеансов: ${rawItems.length}`);

                const groupedItems = groupSessionsByMovieAndDay(rawItems, DAYS_TO_PARSE);
                log.info(`После группировки и фильтрации: ${groupedItems.length} записей`);

                const referer = request.url;
                const tasks = [];
                const coverPathByUrl = new Map();

                const uniqueSites = Array.from(
                    new Set(groupedItems.map((s) => s.site))
                ).filter(isValidHttpUrl);
                for (const movieUrl of uniqueSites) {
                    if (descCache.has(movieUrl)) continue;
                    const cookieString = getCookieStringSafe(
                        session,
                        movieUrl,
                        referer
                    );
                    tasks.push(
                        (async () => {
                            const desc = await fetchMovieDescription(
                                movieUrl,
                                cookieString,
                                referer
                            );
                            descCache.set(movieUrl, desc || "");
                        })()
                    );
                }

                for (const s of groupedItems) {
                    const coverUrl = s._coverUrl;
                    if (!coverUrl) continue;
                    const hash = crypto
                        .createHash("md5")
                        .update(coverUrl)
                        .digest("hex")
                        .slice(0, 8);
                    const fileBase = `${s.slug}-${hash}`;
                    const cookieString = getCookieStringSafe(
                        session,
                        coverUrl,
                        referer
                    );
                    tasks.push(
                        (async () => {
                            try {
                                const p = await downloadImage(
                                    coverUrl,
                                    IMAGES_DIR,
                                    fileBase,
                                    referer,
                                    cookieString
                                );
                                coverPathByUrl.set(coverUrl, p);
                            } catch (e) {
                                log.warning(
                                    `Не удалось скачать обложку: ${coverUrl} -> ${e.message}`
                                );
                            }
                        })()
                    );
                }

                await Promise.allSettled(tasks);

                for (const s of groupedItems) {
                    if (
                        !s.description &&
                        isValidHttpUrl(s.site) &&
                        descCache.has(s.site)
                    )
                        s.description = descCache.get(s.site) || "";
                }

                for (const s of groupedItems) {
                    try {
                        // Отнимаем 1 час от исходного времени dateStart перед отправкой в базу
                        const dateStartShifted = new Date(new Date(s.dateStart).getTime() - 60 * 60 * 1000).toISOString();
                        const party = {
                            title: s.title,
                            abbTitle: s.abbtitle,
                            slug: s.slug,
                            dateStart: dateStartShifted,
                            site: s.site,
                            tel: "",
                            description: s.description || "",
                            allShowTimes: s.allShowTimes,
                        };
                        const saved = await strapi.upsertParty(party);
                        const partyId = saved?.id;
                        if (!partyId) continue;

                        if (s._coverUrl) {
                            const localPath = coverPathByUrl.get(s._coverUrl);
                            if (localPath) {
                                try {
                                    await strapi.uploadAndLinkFile(
                                        localPath,
                                        s._coverUrl,
                                        s.title,
                                        partyId,
                                        "cover"
                                    );
                                    log.info(
                                        `Cover успешно привязан к party=${partyId}`
                                    );
                                } catch (e) {
                                    log.warning(
                                        `Не удалось привязать cover: ${e.message}`
                                    );
                                }
                            }
                        }
                    } catch (e) {
                        log.warning(
                            `Strapi upsert failed for "${s.title}" (${s.slug}): ${describeFetchError(e)}`
                        );
                    }
                }

                for (const s of groupedItems) delete s._coverUrl;
                sessions.push(...groupedItems);
            },
        },
        config
    );

    await crawler.run([CINEMA_URL]);
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});

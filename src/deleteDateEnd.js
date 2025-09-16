// clear-dateEnd-filtered.js
// package.json -> { "type": "module" }
// node clear-dateEnd-filtered.js

import 'dotenv/config';

const STRAPI_URL = (process.env.STRAPI_URL || 'http://127.0.0.1:1337').replace(/\/+$/, '');
const STRAPI_TOKEN = process.env.STRAPI_TOKEN || '';
const COLLECTION = 'parties';
const LOCALE = process.env.STRAPI_LOCALE || ''; // если используете i18n
const PUBLICATION_STATE = 'preview'; // 'live' для опубликованных, 'preview' для draft+published

async function fetchJson(path, qs = '') {
  const url = `${STRAPI_URL}${path}${qs}`;
  const res = await fetch(url, {
    headers: STRAPI_TOKEN
      ? { Authorization: `Bearer ${STRAPI_TOKEN}` }
      : {},
  });
  if (!res.ok) throw new Error(`GET ${url} -> ${res.status}`);
  return res.json();
}

async function putJson(path, body, qs = '') {
  const url = `${STRAPI_URL}${path}${qs}`;
  const res = await fetch(url, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      ...(STRAPI_TOKEN ? { Authorization: `Bearer ${STRAPI_TOKEN}` } : {}),
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`PUT ${url} -> ${res.status}\n${text}`);
  }
  return res.json();
}
//658 - 814
async function main() {
  // console.log(`Поиск записей ID 658-841...`);
  const params = new URLSearchParams();
  params.set('filters[id][$gte]', '758');
  params.set('filters[id][$lte]', '814');
  params.set('pagination[pageSize]', '100');
  params.set('publicationState', PUBLICATION_STATE); // для draft+published
  if (LOCALE) params.set('locale', LOCALE);
  const qsList = `?${params.toString()}`;
  
  // 1) Получить существующие записи
  const list = await fetchJson(`/api/${COLLECTION}`, qsList);
  const items = list?.data || [];
  if (!items.length) {
    console.log('Нет записей в этом диапазоне (проверьте фильтры, локаль и publicationState).');
    return;
  }
  
  console.log(`Найдено ${items.length} записей, очищаем dateEnd...`);
  let success = 0, fail = 0;

  for (const item of items) {
    const numericId = item.id; // числовой id для лога
    const docId = item.documentId; // строковый documentId для PUT
    if (!docId) {
      console.error(`❌ ID ${numericId}: documentId не найден в ответе`);
      fail++;
      continue;
    }
    
    try {
      const qsPut = LOCALE ? `?locale=${LOCALE}` : '';
      await putJson(`/api/${COLLECTION}/${docId}`, { data: { dateEnd: null } }, qsPut);
      console.log(`✅ ID ${numericId} (documentId: ${docId}) очищен`);
      success++;
    } catch (e) {
      console.error(`❌ ID ${numericId} (documentId: ${docId}) -> ${e.message}`);
      fail++;
    }
  }

  console.log(`\nРезультат: очищено ${success}, ошибок ${fail}`);
}

main().catch(e => {
  console.error('Ошибка скрипта:', e);
  process.exit(1);
});

# ym_sanok.ads_settings

## Назначение

Актуальный снимок настроек объявлений Яндекс Директа. Используется для:
- ответов на «какие объявления в группе X?», «покажи тексты объявлений», «какие отклонены модерацией и почему?»;
- креативного аудита: тексты, изображения, расширения, ответственные ссылки для динамических/смарт-объявлений;
- связки с `dm_direct_performance` через `ad_group_id` (в витрине нет `ad_id`, статистика ведётся на уровне групп; для per-ad нужен отдельный мартет — вне этого запроса).

## Источник данных и обновление

- Источник: `ym_sanok.ads_meta`
- **Тип MV**: INSERT-triggered
- **Движок target**: `ReplacingMergeTree(loaded_at)`
- **Ключ дедупа**: `ORDER BY (campaign_id, ad_group_id, ad_id)` → одна строка на объявление

### Как запрашивать

```sql
SELECT ... FROM ym_sanok.ads_settings FINAL WHERE ...
```

## Структура таблицы

### Служебное и идентификация
| Поле | Тип | Описание |
|------|-----|----------|
| loaded_at | DateTime | Версия снимка (ReplacingMergeTree) |
| ad_id | Int64 | ID объявления |
| ad_group_id | Int64 | ID группы (JOIN к `adgroups_settings.group_id`) |
| campaign_id | Int64 | ID кампании |
| status | LowCardinality(String) | ACCEPTED / DRAFT / MODERATION / REJECTED |
| state | LowCardinality(String) | ON / OFF / SUSPENDED |
| status_clarification | String | Причина отклонения / ограничения (человеко-читаемая) |
| ad_type | LowCardinality(String) | TEXT_AD / IMAGE_AD / CPM_BANNER_AD / CPC_VIDEO_AD / SHOPPING_AD |
| ad_subtype | LowCardinality(String) | NONE / TEXT_IMAGE_AD / RESPONSIVE_AD / MOBILE_APP_AD и др. |
| age_label | LowCardinality(String) | Возрастная маркировка (18+, 16+...) |
| ad_categories | Array(String) | Товарные категории модерации |

### Креатив: TEXT_AD
| Поле | Тип | Описание |
|------|-----|----------|
| title | String | Заголовок 1 |
| title2 | String | Заголовок 2 |
| text | String | Текст объявления |
| href | String | Ссылка (короткая) |
| final_url | String | Финальный URL (после редиректов) |
| display_domain | String | Отображаемый домен |
| display_url_path | String | Отображаемый путь |

### Креатив: IMAGE_AD / TEXT_IMAGE_AD
| Поле | Тип | Описание |
|------|-----|----------|
| image_ad_title | String | Заголовок 1 картиночного |
| image_ad_title2 | String | Заголовок 2 |
| image_ad_text | String | Текст |
| image_ad_href | String | Ссылка |
| image_ad_final_url | String | Финальный URL |
| image_ad_image_hash | String | Хэш изображения |

### Креатив: RESPONSIVE / SMART (для динамики каталога sanok)
| Поле | Тип | Описание |
|------|-----|----------|
| responsive_titles_json | String | JSON-массив заголовков responsive-объявления |
| responsive_texts_json | String | JSON-массив текстов |
| responsive_href | String | Ссылка |
| responsive_final_url | String | Финальный URL |
| responsive_display_domain | String | Отображаемый домен |
| responsive_display_url_path | String | Отображаемый путь |
| smart_creative_json | String | JSON-конфиг смарт-креатива |

### Расширения
| Поле | Тип | Описание |
|------|-----|----------|
| sitelink_set_id | Nullable(Int64) | ID набора быстрых ссылок |
| vcardid | Nullable(Int64) | ID виртуальной визитки |
| business_id | Nullable(Int64) | ID организации в Я.Бизнесе |
| turbo_page_id | Nullable(Int64) | ID турбо-страницы |
| tracking_phone_id | Nullable(Int64) | ID отслеживаемого телефона |
| ad_extension_ids | Array(Int64) | ID дополнительных расширений |
| ad_extension_types | Array(String) | Типы расширений |

### Модерация
| Поле | Тип | Описание |
|------|-----|----------|
| vcard_moderation | LowCardinality(String) | ACCEPTED / REJECTED / MODERATION |
| ad_image_moderation | LowCardinality(String) | Статус модерации картинки |
| sitelinks_moderation | LowCardinality(String) | Статус модерации сайтлинков |
| display_url_path_moderation | LowCardinality(String) | Статус модерации отображаемого пути |
| turbo_page_moderation | LowCardinality(String) | Статус модерации турбо-страницы |

## Сценарии использования

### 1. Все отклонённые объявления с причиной

```sql
SELECT ad_id, campaign_id, ad_group_id, title, text, status, status_clarification
FROM ym_sanok.ads_settings FINAL
WHERE status = 'REJECTED'
ORDER BY campaign_id, ad_group_id;
```

### 2. Объявления в группе X

```sql
SELECT ad_id, ad_type, status, state, title, title2, text, final_url
FROM ym_sanok.ads_settings FINAL
WHERE ad_group_id = <id>
ORDER BY state, status;
```

### 3. Поиск текста в объявлениях

```sql
SELECT ad_id, campaign_id, title, title2, text
FROM ym_sanok.ads_settings FINAL
WHERE state = 'ON'
  AND (title ILIKE '%акция%' OR text ILIKE '%акция%');
```

### 4. Распределение по типам креатива

```sql
SELECT ad_type, ad_subtype, status, count() AS n
FROM ym_sanok.ads_settings FINAL
GROUP BY ad_type, ad_subtype, status
ORDER BY n DESC;
```

### 5. Объявления с турбо-страницами / витринами

```sql
SELECT ad_id, ad_group_id, title, turbo_page_id, turbo_page_moderation
FROM ym_sanok.ads_settings FINAL
WHERE turbo_page_id IS NOT NULL;
```

## Таблица триггеров

| Вопрос агента | Что смотреть |
|---------------|--------------|
| «Покажи объявления группы X» | `WHERE ad_group_id = ...` |
| «Почему отклонено» | `status_clarification` + `*_moderation` |
| «Тексты объявлений» | `title`, `title2`, `text` (или `image_ad_*`, `responsive_*_json`) |
| «Куда ведёт объявление» | `final_url`, `href` |
| «Расширения (сайтлинки, визитка)» | `sitelink_set_id`, `vcardid`, `business_id` |
| «Турбо-страницы» | `turbo_page_id`, `turbo_page_moderation` |
| «Смарт/динамические креативы» | `smart_creative_json`, `responsive_*_json` |

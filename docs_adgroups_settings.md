# ym_sanok.adgroups_settings

## Назначение

Актуальный снимок настроек групп объявлений Яндекс Директа. Используется для:
- ответов на «какие ключи в группе X?», «какой автотаргетинг у группы Y?», «в каких регионах показы?»;
- связи между `dm_direct_performance` (где есть `adgroup_id`) и конкретными настройками (ключи, автотаргетинг, фид);
- специфики sanok: понять, какие группы работают на ручных ключах, какие на автотаргетинге, какие через товарный фид (динамические/смарт-объявления для каталога сантехники).

## Источник данных и обновление

- Источник: `ym_sanok.ad_groups_meta`
- **Тип MV**: INSERT-triggered
- **Движок target**: `ReplacingMergeTree(loaded_at)`
- **Ключ дедупа**: `ORDER BY (campaign_id, group_id)` → одна строка на группу
- При перезаливе `ad_groups_meta` target автоматически получает новую версию

### Как запрашивать

```sql
SELECT ... FROM ym_sanok.adgroups_settings FINAL WHERE ...
```

## Структура таблицы

### Служебное и идентификация
| Поле | Тип | Описание |
|------|-----|----------|
| loaded_at | DateTime | Версия снимка (ReplacingMergeTree) |
| group_id | Int64 | ID группы — JOIN-ключ с `dm_direct_performance.adgroup_id` |
| group_name | String | Название группы |
| campaign_id | Int64 | ID кампании — JOIN к `campaigns_settings` |
| status | LowCardinality(String) | ACCEPTED / DRAFT / MODERATION / REJECTED |
| serving_status | LowCardinality(String) | ELIGIBLE / RARELY_SERVED / UNSERVABLE и др. — показывается ли группа сейчас |
| group_type | LowCardinality(String) | BASE (ручные ключи) / DYNAMIC (парсинг сайта) / SMART (смарт-баннеры) / FEED / CPM_* / MOBILE_APP и др. |
| group_subtype | LowCardinality(String) | Уточнение подтипа |

### Таргетинг
| Поле | Тип | Описание |
|------|-----|----------|
| region_ids | Array(Int64) | ID регионов показа |
| restricted_region_ids | Array(Int64) | Регионы с ограничениями |
| negative_keywords | Array(String) | Минус-слова уровня группы |
| negative_kw_shared_set_ids | Array(Int64) | ID общих наборов минус-слов |

### Ключевые фразы
| Поле | Тип | Описание |
|------|-----|----------|
| keywords | Array(String) | Массив ключевых фраз (ручной ввод) |
| keyword_count | Int32 | Кол-во ключей (быстрая проверка: 0 = группа без ручных ключей) |

### Автотаргетинг
| Поле | Тип | Описание |
|------|-----|----------|
| autotargeting_state | LowCardinality(String) | ON / SUSPENDED / OFF — включён ли вообще |
| autotargeting_status | LowCardinality(String) | ACCEPTED / DRAFT / MODERATION / REJECTED |
| autotargeting_exact | LowCardinality(String) | ACTIVE / SUSPENDED — точное соответствие запросов |
| autotargeting_alternative | LowCardinality(String) | ACTIVE / SUSPENDED — альтернативные запросы |
| autotargeting_competitor | LowCardinality(String) | ACTIVE / SUSPENDED — запросы конкурентов |
| autotargeting_broader | LowCardinality(String) | ACTIVE / SUSPENDED — более широкие запросы |
| autotargeting_accessory | LowCardinality(String) | ACTIVE / SUSPENDED — сопутствующие |
| autotargeting_brand_without | LowCardinality(String) | ACTIVE / SUSPENDED — брендовые без упоминания рекламодателя |
| autotargeting_brand_with_advertiser | LowCardinality(String) | ACTIVE / SUSPENDED — брендовые с упоминанием |

### Динамические объявления и фид (важно для sanok — каталог сантехники)
| Поле | Тип | Описание |
|------|-----|----------|
| dynamic_domain_url | String | URL домена-источника для DYNAMIC |
| dynamic_domain_url_status | LowCardinality(String) | Статус валидации домена |
| feed_id | Nullable(Int64) | ID товарного фида |
| feed_source_type | LowCardinality(String) | Тип источника фида |
| feed_source_status | LowCardinality(String) | Статус обработки фида |
| feed_ad_title_source | LowCardinality(String) | Источник заголовков |
| feed_ad_body_source | LowCardinality(String) | Источник описаний |
| feed_category_ids | Array(Int64) | Отфильтрованные категории фида |

## Сценарии использования

### 1. Группы на ручных ключах vs автотаргетинге

```sql
SELECT
    multiIf(keyword_count > 0 AND autotargeting_state = 'ON', 'keys+auto',
            keyword_count > 0, 'keys_only',
            autotargeting_state = 'ON', 'auto_only',
            'neither') AS setup,
    count() AS groups
FROM ym_sanok.adgroups_settings FINAL
WHERE status = 'ACCEPTED'
GROUP BY setup;
```

### 2. Группы через фид (динамика/смарт)

```sql
SELECT group_id, group_name, campaign_id, group_type, feed_id, feed_category_ids
FROM ym_sanok.adgroups_settings FINAL
WHERE feed_id IS NOT NULL
ORDER BY campaign_id, group_id;
```

### 3. Связь настроек со статистикой группы

```sql
SELECT
    g.group_name, g.group_type, g.keyword_count, g.autotargeting_state,
    sum(p.cost)             AS cost,
    sum(p.clicks)           AS clicks,
    sum(p.order_paid)       AS orders,
    sum(p.purchase_revenue) AS revenue
FROM ym_sanok.dm_direct_performance FINAL p
JOIN ym_sanok.adgroups_settings FINAL g ON p.adgroup_id = g.group_id
WHERE p.date >= today() - 30
GROUP BY g.group_name, g.group_type, g.keyword_count, g.autotargeting_state
ORDER BY cost DESC
LIMIT 30;
```

### 4. Какие категории автотаргета включены чаще всего

```sql
SELECT
    sum(autotargeting_exact='ACTIVE')       AS exact,
    sum(autotargeting_alternative='ACTIVE') AS alternative,
    sum(autotargeting_competitor='ACTIVE')  AS competitor,
    sum(autotargeting_broader='ACTIVE')     AS broader,
    sum(autotargeting_accessory='ACTIVE')   AS accessory,
    sum(autotargeting_brand_without='ACTIVE') AS brand_without,
    sum(autotargeting_brand_with_advertiser='ACTIVE') AS brand_with
FROM ym_sanok.adgroups_settings FINAL
WHERE autotargeting_state = 'ON';
```

### 5. Ключевые фразы конкретной группы

```sql
SELECT group_name, keyword_count, keywords
FROM ym_sanok.adgroups_settings FINAL
WHERE group_id = <id>;
```

## Таблица триггеров

| Вопрос агента | Что смотреть |
|---------------|--------------|
| «Ключи группы X» | `keywords`, `keyword_count` |
| «Автотаргетинг включён?» | `autotargeting_state`, категории `autotargeting_*` |
| «Группы через фид» | `feed_id IS NOT NULL`, `group_type='DYNAMIC'/'SMART'` |
| «Регионы показа» | `region_ids` |
| «Минус-слова группы» | `negative_keywords`, `negative_kw_shared_set_ids` |
| «Группа показывается?» | `serving_status = 'ELIGIBLE'` |

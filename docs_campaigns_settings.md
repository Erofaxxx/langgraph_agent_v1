# ym_sanok.campaigns_settings

## Назначение

Срез актуальных настроек рекламных кампаний Яндекс Директа для sanok. Используется для:
- сопоставления статистики из `dm_direct_performance` с настройками (стратегия, бюджет, цели);
- ответов на вопросы вида «какая стратегия у кампании X?», «сколько денег на балансе?», «какие цели у автостратегии?»;
- аудита кампаний: поиск отключенных, без бюджета, с неправильной атрибуцией, с неверно настроенным автотаргетингом.

## Источник данных и обновление

- Источник: `ym_sanok.campaigns_meta` (выгрузка настроек кампаний из Direct API)
- **Тип MV**: INSERT-triggered (срабатывает на каждый INSERT в `campaigns_meta`)
- **Движок target-таблицы**: `ReplacingMergeTree(loaded_at)`
- **Ключ дедупа**: `ORDER BY campaign_id` → одна строка на кампанию, версия = `loaded_at`
- При каждом перезаливе `campaigns_meta` в target прилетает новая версия строки, при мёрдже старые версии схлопываются

### Как запрашивать

```sql
-- всегда с FINAL, чтобы получить актуальный снимок
SELECT ... FROM ym_sanok.campaigns_settings FINAL WHERE ...
```

Без `FINAL` в моменте между двумя загрузками можно получить дубли. Для больших выборок можно использовать `argMax` вместо `FINAL`.

## Структура таблицы

### Служебное
| Поле | Тип | Описание |
|------|-----|----------|
| loaded_at | DateTime | Версия снимка — используется `ReplacingMergeTree` |

### Идентификация
| Поле | Тип | Описание |
|------|-----|----------|
| campaign_id | Int64 | ID кампании — JOIN-ключ с `dm_direct_performance`, `adgroups_settings` |
| campaign_name | String | Название кампании |
| campaign_type | LowCardinality(String) | TEXT_CAMPAIGN / DYNAMIC_TEXT_CAMPAIGN / SMART_CAMPAIGN и др. |
| status | LowCardinality(String) | ACCEPTED / DRAFT / MODERATION / REJECTED |
| state | LowCardinality(String) | ON / OFF / SUSPENDED / ENDED / ARCHIVED |
| status_payment | LowCardinality(String) | Статус оплаты |
| currency | LowCardinality(String) | Валюта счёта |
| start_date | Date | Дата старта |
| end_date | Nullable(Date) | Дата завершения (если задана) |

### Бюджет и остатки
| Поле | Тип | Описание |
|------|-----|----------|
| daily_budget_amount | Nullable(Decimal) | Дневной лимит (руб) |
| funds_mode | LowCardinality(String) | SHARED_ACCOUNT_FUNDS / CAMPAIGN_FUNDS |
| funds_sum | Nullable(Decimal) | Заведено средств за всё время |
| funds_balance | Nullable(Decimal) | Текущий остаток |
| funds_balance_bonus | Nullable(Decimal) | Бонусный остаток |
| funds_shared_spend | Nullable(Decimal) | Расход из общего счёта |

### Стратегия поиска
| Поле | Тип | Описание |
|------|-----|----------|
| strategy_search_type | LowCardinality(String) | HIGHEST_POSITION / WB_MAXIMUM_CLICKS / AVERAGE_CPC / AVERAGE_CPA_MULTIPLE_GOALS / AVERAGE_CRR / WB_MAXIMUM_CONVERSION_RATE / SERVING_OFF и др. |
| strategy_search_weekly_budget | Nullable(Decimal) | Недельный бюджет |
| strategy_search_bid_ceiling | Nullable(Decimal) | Максимальная CPC |
| strategy_search_average_cpc | Nullable(Decimal) | Целевой CPC (для AVERAGE_CPC) |
| strategy_search_average_cpa | Nullable(Decimal) | Целевой CPA (для AVERAGE_CPA_*) |
| strategy_search_goal_id | Nullable(Int64) | Цель оптимизации (JOIN к счётчику Метрики) |
| strategy_search_crr | Nullable(Int32) | Доля рекламных расходов (для AVERAGE_CRR) — ключевая для ecommerce |
| strategy_search_roi_coef | Nullable(Decimal) | Коэффициент ROI |
| strategy_search_reserve_return | Nullable(Int32) | Запасной возврат (для CRR) |
| strategy_search_clicks_per_week | Nullable(Int64) | Максимум кликов в неделю |

### Стратегия РСЯ
| Поле | Тип | Описание |
|------|-----|----------|
| strategy_network_type | LowCardinality(String) | SERVING_OFF / NETWORK_DEFAULT и др. |
| strategy_network_weekly_budget | Nullable(Decimal) | Недельный бюджет РСЯ |
| strategy_network_bid_ceiling | Nullable(Decimal) | Максимальная CPC в РСЯ |
| strategy_network_average_cpc | Nullable(Decimal) | Целевой CPC |
| strategy_network_average_cpa | Nullable(Decimal) | Целевой CPA |
| strategy_network_goal_id | Nullable(Int64) | Цель для РСЯ-автостратегии |
| strategy_network_limit_percent | Nullable(Int32) | Лимит показов в РСЯ, % |

### Атрибуция, счётчики, цели
| Поле | Тип | Описание |
|------|-----|----------|
| attribution_model | LowCardinality(String) | LAST_CLICK / FIRST_CLICK / LSC / LSCCD / AUTO |
| counter_ids | Array(Int64) | ID счётчиков Метрики кампании (обычно `178943` и/или `63025594`) |
| priority_goal_ids | Array(Int64) | ID целей с приоритетами |
| priority_goal_values | Array(Decimal) | Веса приоритетных целей (синхронно с `priority_goal_ids`) |
| relevant_keywords_budget_percent | Nullable(Int32) | Бюджет на релевантные фразы, % |
| relevant_keywords_goal_id | Nullable(Int64) | Цель для подбора релевантных фраз |

### Таргетинг
| Поле | Тип | Описание |
|------|-----|----------|
| negative_keywords | Array(String) | Минус-слова кампании |
| excluded_sites | Array(String) | Запрещённые площадки (РСЯ) |
| blocked_ips | Array(String) | Заблокированные IP |
| time_targeting_schedule | Array(String) | Расписание показов по часам (24 строки) |
| package_bidding_strategy_id | Nullable(Int64) | ID пакетной стратегии |

## Сценарии использования

### 1. Все активные кампании

```sql
SELECT campaign_id, campaign_name, strategy_search_type, strategy_network_type,
       daily_budget_amount, funds_balance, attribution_model
FROM ym_sanok.campaigns_settings FINAL
WHERE status = 'ACCEPTED' AND state = 'ON'
ORDER BY campaign_name;
```

### 2. Где кончаются деньги

```sql
SELECT campaign_name, funds_balance, daily_budget_amount
FROM ym_sanok.campaigns_settings FINAL
WHERE state = 'ON' AND funds_balance IS NOT NULL
ORDER BY funds_balance ASC
LIMIT 10;
```

### 3. Связка настроек со статистикой

```sql
SELECT
    s.campaign_name,
    s.strategy_search_type,
    s.strategy_search_crr,
    sum(p.cost)             AS cost,
    sum(p.purchase_revenue) AS revenue,
    sum(p.order_paid)       AS orders
FROM ym_sanok.dm_direct_performance FINAL p
JOIN ym_sanok.campaigns_settings FINAL s USING (campaign_id)
WHERE p.date >= today() - 30
GROUP BY s.campaign_name, s.strategy_search_type, s.strategy_search_crr
ORDER BY cost DESC;
```

### 4. Кампании с автостратегией по CRR (ключевое для ecommerce)

```sql
SELECT campaign_name, strategy_search_crr, strategy_search_goal_id
FROM ym_sanok.campaigns_settings FINAL
WHERE strategy_search_type = 'AVERAGE_CRR'
ORDER BY strategy_search_crr;
```

### 5. Аудит атрибуции и целей

```sql
SELECT attribution_model, count() AS n
FROM ym_sanok.campaigns_settings FINAL
WHERE state = 'ON'
GROUP BY attribution_model;
```

## Таблица триггеров

| Вопрос агента | Что смотреть |
|---------------|--------------|
| «Активные кампании» | `status='ACCEPTED' AND state='ON'` |
| «Где деньги кончаются» | `funds_balance ASC` |
| «Стратегии» | `strategy_search_type`, `strategy_network_type` |
| «Бюджет на день» | `daily_budget_amount` |
| «Счётчики кампании» | `counter_ids` |
| «Цели оптимизации» | `strategy_search_goal_id`, `strategy_network_goal_id`, `priority_goal_ids` |
| «Минус-слова» | `negative_keywords` |
| «Расписание показов» | `time_targeting_schedule` |

# campaign_dict — Sanok

## Назначение

Словарь перевода UTM-кампаний из транслитерации (латиница) в человекочитаемые названия (кириллица). Яндекс Директ записывает UTMCampaign транслитом — эта таблица декодирует их для отчётов и AI-агента.

## Обновление

Статическая таблица. Обновляется вручную при создании новых кампаний в Директе. Текущее содержание: **28 кампаний**.

## Структура таблицы

```sql
CREATE TABLE ym_sanok.campaign_dict (
    utm_campaign  String,   -- UTMCampaign из визитов (транслит, латиница)
    campaign_name String    -- Название кампании (кириллица)
)
ENGINE = MergeTree ORDER BY utm_campaign
COMMENT 'Словарь перевода UTMCampaign (транслит) -> Кампания (рус)'
```

## Полный справочник (28 записей)

| utm_campaign | campaign_name |
|---|---|
| `Kampaniya_dlya_pozicij_s_potencialom_prodazh` | Кампания для позиций с потенциалом продаж |
| `MK_Tovarnaya_Centr_bez_MO` | МК_Товарная_Центр_без_МО |
| `MK_Tovarnaya_Msk_MO` | МК_Товарная_Мск_МО |
| `MK_\|_Tovarnaya_kampaniya_\|_CFO_SPb_MSK` | МК \| Товарная кампания \| ЦФО. СПб -МСК |
| `MK_\|_Vanny_moechnye_\|_Moskva_i_obl` | МК \| Ванны моечные \| Москва и обл |
| `Poisk_\|_Brend_\|_Moskva_i_oblast` | Поиск \| Бренд \| Москва и область |
| `Poisk_\|_CHasha_genuya_\|_yur_lica_\|_Moskva_i_obl` | Поиск \| Чаша генуя \| юр.лица \| Москва и обл |
| `Poisk_\|_Detskie_unitazy_\|_Moskva_i_obl` | Поиск \| Детские унитазы \| Москва и обл |
| `Poisk_\|_Italyanskie_brendy_\|_Moskva_i_obl` | Поиск \| Итальянские бренды \| Москва и обл |
| `Poisk_\|_Mojka_dlya_obuvi_\|_Moskva_i_obl` | Поиск \| Мойка для обуви \| Москва и обл |
| `Poisk_\|_Sensornye_pissuary_\|_yur_lica_\|_Moskva_i_obl` | Поиск \| Сенсорные писсуары \| юр.лица \| Москва и обл |
| `Poisk_\|_Tovarnaya_galereya_Aksessuary_i_sovr_resheniya_\|_Fid_\|_Moskva_i_obl` | Поиск \| Товарная галерея (Аксессуары и совр. решения) \| Фид \| Москва и обл |
| `Poisk_\|_Tovarnaya_galereya_\|_Fid_\|_Detskie_unitazy_\|_Moskva_i_obl` | Поиск \| Товарная галерея \| Фид \| Детские унитазы \| Москва и обл |
| `Poisk_\|_Tovarnaya_galereya_\|_Fid_\|_Dush_\|_Moskva_i_obl` | Поиск \| Товарная галерея \| Фид \| Душ \| Москва и обл |
| `Poisk_\|_Tovarnaya_galereya_\|_Fid_\|_Moechnye_vanny_\|_Moskva_i_obl` | Поиск \| Товарная галерея \| Фид \| Моечные ванны \| Москва и обл |
| `Poisk_\|_Tovarnaya_galereya_\|_Stranicy_kataloga_\|_Detskie_unitazy_\|_Moskva_i_obl` | Поиск \| Товарная галерея \| Страницы каталога \| Детские унитазы \| Москва и обл |
| `Poisk_\|_Vanny_2_0\|_Moskva_i_obl` | Поиск \| Ванны 2.0\| Москва и обл |
| `Poisk_\|_Vanny_i_unitazy_dlya_invalidov_\|_Moskva_i_obl` | Поиск \| Ванны и унитазы (для инвалидов) \| Москва и обл |
| `Poisk_\|_Viduary_avarijnye_dushi_\|_Moskva_i_obl` | Поиск \| Видуары. аварийные души \| Москва и обл |
| `Smart_bannery_\|_B2B_\|_Moskva` | Смарт-баннеры \| B2B \| Москва |
| `Smart_bannery_\|_Detskie_unitazy_\|_Moskva_i_obl` | Смарт-баннеры \| Детские унитазы \| Москва и обл |
| `Smart_bannery_\|_LaL_2_zayavka_\|_Moskva_i_MO` | Смарт-баннеры \| LaL 2 заявка \| Москва и МО |
| `Smart_bannery_\|_Moechnye_vanny_\|_Moskva_i_obl` | Смарт-баннеры \| Моечные ванны \| Москва и обл |
| `Smart_bannery_\|_Retargeting_\|_Moskva_i_MO_new` | Смарт-баннеры \| Ретаргетинг \| Москва и МО_new |
| `Smart_bannery_\|_Santekhnika_dlya_invalidov_\|_Centr_bez_MO` | Смарт-баннеры \| Сантехника для инвалидов \| Центр без МО |
| `Smart_bannery_\|_Vanny_i_unitazy_\|_Moskva_i_obl` | Смарт-баннеры \| Ванны и унитазы \| Москва и обл |
| `Tovarnaya_gallereya_\|_CHasha_genuya_\|_Moskva_i_obl` | Товарная галлерея \| Чаша генуя \| Москва и обл |
| `mk_okt23` | mk_okt23 |

## Типы кампаний (по префиксу названия)

| Префикс | Тип | Кампаний |
|---------|-----|---------|
| `Poisk \|` | Поисковые кампании | 13 |
| `Smart-баннеры \|` | Смарт-баннеры (ретаргетинг, LAL) | 7 |
| `МК` | Мастер кампаний (товарные) | 4 |
| Другие | Разные | 4 |

## Сценарии использования для AI-агента

### 1. Перевод UTM-кампании в читаемое название

**Триггеры**: "Что за кампания Poisk_|_Brend...?", "Переведи utm_campaign", "Название кампании"

```sql
SELECT utm_campaign, campaign_name
FROM ym_sanok.campaign_dict
WHERE utm_campaign = 'Poisk_|_Brend_|_Moskva_i_oblast'
```

### 2. Трафик с читаемыми названиями кампаний

**Триггеры**: "Трафик по кампаниям с нормальными названиями", "Отчёт по кампаниям (расход, выручка)"

```sql
SELECT
    coalesce(nullIf(d.campaign_name, ''), t.utm_campaign) AS campaign,
    sum(t.visits)                                          AS visits,
    sum(t.sessions_with_purchase)                          AS purchases,
    round(sum(t.revenue), 0)                               AS revenue
FROM ym_sanok.dm_traffic_performance AS t
LEFT JOIN ym_sanok.campaign_dict AS d ON t.utm_campaign = d.utm_campaign
WHERE t.utm_campaign != ''
GROUP BY campaign
ORDER BY revenue DESC
```

### 3. Конверсионные пути с расшифрованными кампаниями

**Триггеры**: "Через какие кампании клиенты приходят к покупке?", "Цепочки кампаний"

```sql
SELECT
    cp.client_id,
    cp.revenue,
    cp.path_length,
    arrayMap(
        c -> coalesce(
            (SELECT campaign_name FROM ym_sanok.campaign_dict WHERE utm_campaign = c LIMIT 1),
            c
        ),
        cp.campaigns_path
    ) AS campaigns_readable
FROM ym_sanok.dm_conversion_paths AS cp
WHERE cp.converted = 1
ORDER BY cp.revenue DESC
LIMIT 20
```

### 4. Заказы с читаемыми названиями last-touch кампании

**Триггеры**: "Заказы по кампаниям", "Какая кампания приносит больше заказов?"

```sql
SELECT
    coalesce(nullIf(d.campaign_name, ''), o.utm_campaign_last) AS campaign,
    count()                          AS orders,
    round(sum(o.order_revenue), 0)   AS revenue,
    round(avg(o.days_to_purchase), 0) AS avg_days_to_buy
FROM ym_sanok.dm_orders AS o
LEFT JOIN ym_sanok.campaign_dict AS d ON o.utm_campaign_last = d.utm_campaign
WHERE o.utm_campaign_last != ''
GROUP BY campaign
ORDER BY revenue DESC
```

## Технические уточнения

**Связь с таблицами**: `campaign_dict.utm_campaign` матчится с полем `utm_campaign` / `UTMCampaign` в `dm_traffic_performance`, `dm_campaign_funnel`, `dm_client_profile` (last_utm_campaign), `dm_conversion_paths` (campaigns_path[]), `dm_orders` (utm_campaign_last / utm_campaign_first), `dm_client_journey` (utm_campaign).

**Не путать с campaigns_settings**: `campaign_dict` переводит UTM-метки (из визитов Метрики). `campaigns_settings` хранит настройки кампаний Директа (campaign_id, стратегия, бюджет). Связь между ними — через `campaign_name` (приблизительная) или через `dm_direct_performance` (campaign_id + campaign_name).

**Неполное покрытие**: 28 записей покрывают основные кампании. Если в визитах встречается `utm_campaign`, которого нет в словаре — используй оригинальный транслит. Паттерн: `coalesce(nullIf(d.campaign_name, ''), t.utm_campaign)`.

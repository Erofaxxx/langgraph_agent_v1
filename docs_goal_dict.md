# goal_dict — Sanok

## Назначение

Справочник целей Яндекс Метрики для счётчиков sanok (178943 / 63025594). Переводит числовой `goal_id` в человекочитаемое название, категорию и описание триггера. Используется AI-агентом для декодирования goal_id из других таблиц.

## Обновление

Статическая таблица. Обновляется вручную при добавлении/удалении целей в Метрике. Текущее содержание: **65 целей**.

## Структура таблицы

```sql
CREATE TABLE ym_sanok.goal_dict (
    goal_id       UInt64,              -- ID цели Метрики
    goal_name     String,              -- Человекочитаемое название
    goal_category LowCardinality(String), -- Категория (см. ниже)
    goal_trigger  String               -- Что вызывает достижение цели
)
ENGINE = MergeTree ORDER BY goal_id
```

## Категории целей

| Категория | Кол-во | Описание |
|-----------|--------|----------|
| `транзакция` | 7 | Покупка, заказ, предоплата/постоплата, доставка, отмена |
| `воронка` | 12 | Просмотр товара → корзина → чекаут (шаги до покупки) |
| `звонки` | 4 | Calltouch: уникальные, целевые, уникально-целевые |
| `категория` | 19 | Посещение категорийных страниц (ванны, унитазы, смесители…) |
| `jivo` | 7 | Онлайн-чат Jivo: диалог, приглашение, формы контактов |
| `автоцель` | 10 | Автоматические цели Метрики: клики по телефону, email, формы |
| `прочее` | 6 | Страницы контактов, акций, оптом; шумовые цели |

## Полный справочник

### Транзакция (7 целей)
| goal_id | goal_name | goal_trigger |
|---------|-----------|-------------|
| 3000178943 | Ecommerce: покупка | совершение покупки |
| 31297300 | Заказ оформлен | url: /index.php?route=checkout/success |
| 543662393 | Успешное оформление заказа | составная: order_prepaid + order_postpaid |
| 543662395 | Заказ с предоплатой | идентификатор: order_prepaid |
| 543662396 | Заказ с постоплатой | идентификатор: order_postpaid |
| 543662401 | Отмененный заказ | идентификатор: order_cancelled |
| 543662402 | Заказ доставлен | идентификатор: order_delivered |

### Воронка (12 целей)
| goal_id | goal_name | goal_trigger |
|---------|-----------|-------------|
| 543662405 | Просмотр товара | идентификатор: view_product_details |
| 543662406 | Клик по товару | идентификатор: product_click |
| 543662407 | Просмотр списка товаров | идентификатор: product_list_view |
| 543662409 | Просмотр характеристик товара | идентификатор: specifications_click |
| 543662410 | Изменение характеристик товара | идентификатор: select_characteristic |
| 194388760 | Ecommerce: добавление в корзину | добавление товара в корзину |
| 543662403 | Добавить в корзину | идентификатор: add_to_cart |
| 543662404 | Удалить из корзины | идентификатор: remove_from_cart |
| 21115645 | Корзина | url: /cart |
| 21115915 | Оформление заказа | url: /simplecheckout |
| 543662408 | Переход на чекаут | идентификатор: checkout_open |
| 495725161 | Автоцель: Начало оформления заказа | начало оформления заказа |

### Звонки (4 цели)
| goal_id | goal_name | goal_trigger |
|---------|-----------|-------------|
| 34740969 | Звонок Calltouch | передача данных о звонках |
| 201398152 | Уникальный звонок | первичные звонки (Calltouch) |
| 201398155 | Уникально-целевой звонок | первичные звонки >= 30 с |
| 201398158 | Целевой звонок | звонки >= 30 с |

### Категория (19 целей)
| goal_id | goal_name | goal_trigger |
|---------|-----------|-------------|
| 22763795 | Ванна - раздел | url: vanny/ |
| 22763800 | Ванны чугунные | url: chugunnye/ |
| 22763805 | Ванны стальные | url: stalnye/ |
| 22763810 | Ванны акриловые | url: akrilovye/ |
| 22763815 | Подвесные унитазы с инсталляциями комплекты | url: podvesnie-unitazi-s-installacijami-komplekti/ |
| 22763820 | Раковины для инвалидов | url: rakovini-dlja-invalidov/ |
| 22763825 | Писсуары | url: pissuari/ |
| 22763830 | Унитазы для инвалидов и пожилых | url: unitazi-dlja-invalidov-i-pozhilih-ljudej/ |
| 22763835 | Раковины для инвалидов (2) | url: rakovini-dlja-invalidov/ |
| 22763840 | Комплекты смесителей | url: komplekti-nabori-smesitelej/ |
| 22763845 | Душевые системы | url: dushi-dushevie-sistemi/ |
| 22763850 | Смесители локтевые | url: hirurgicheskie/ |
| 22763855 | Сенсорные смесители | url: sensornye-smesiteli/ |
| 22763860 | Инсталляции для подвесных унитазов | url: dlja-podvesnyh-unitazov/ |
| 22763865 | Поддоны душевые | url: poddoni-dlja-dusha/ |
| 22763935 | Душевые двери / ограждения | url: dushevie-dveri-ograzhdenija/ |
| 22764755 | Шторы для ванной | url: shtori-dlja-vannoj/ |
| 22764760 | Душевые уголки | url: dushevie-ugolki/ |
| 22764765 | Душевые кабины | url: dk/ |

### Jivo (7 целей)
| goal_id | goal_name | goal_trigger |
|---------|-----------|-------------|
| 47284792 | Jivo: установлен диалог | Jivo_Chat_established |
| 47284804 | Jivo: чат запрошен клиентом | Jivo_Chat_requested |
| 47284867 | Jivo: чат начат клиентом | Jivo_Client_initiate_chat |
| 47284936 | Jivo: клиент принял приглашение | Jivo_Proactive_invitation_accepted |
| 47284972 | Jivo: клиент заполнил форму контактов | Jivo_User_gave_contacts_during_chat |
| 47284990 | Jivo: клиент заполнил форму с email | Jivo_In-chat_email_form_submitted |
| 47285017 | Jivo: оффлайн-сообщение | Jivo_Offline_message_sent |

### Автоцель (10 целей)
| goal_id | goal_name | goal_trigger |
|---------|-----------|-------------|
| 175941400 | Автоцель: клик по телефону | клики по номерам телефонов |
| 175941403 | Автоцель: клик по email | клики по email |
| 175941406 | Автоцель: поиск по сайту | использование поиска |
| 175941409 | Автоцель: отправка формы | отправки форм |
| 195990724 | Автоцель: переход в соцсеть | все соцсети |
| 209532193 | Автоцель Jivo: ручное приглашение | клиент ответил на приглашение оператора |
| 211567495 | Автоцель: скачивание файла | скачивания файлов |
| 232782778 | Автоцель: переход в мессенджер | ссылки на мессенджеры |
| 317172960 | Автоцель: заполнил контактные данные | заполнил контактные данные |
| 339159404 | Автоцель: отправил контактные данные | отправил контактные данные |

### Прочее (6 целей)
| goal_id | goal_name | goal_trigger |
|---------|-----------|-------------|
| 21115920 | Контакты | url: kontakti/ |
| 21115925 | Акции | url: akcii/ |
| 130300219 | Посетили сайт | url: sanok.ru (шумовая цель) |
| 157742788 | Более 3-х страниц | просмотр 3 страниц |
| 189636034 | Посетители rgw | url: rgw |
| 337027569 | Оптом - раздел | url: optom |

## Где используются goal_id в других таблицах

| Таблица | Поле | Как связано |
|---------|------|------------|
| `campaigns_settings` | `strategy_search_goal_id` | Цель автостратегии поиска — JOIN `goal_dict` по goal_id |
| `campaigns_settings` | `strategy_network_goal_id` | Цель автостратегии РСЯ — JOIN `goal_dict` по goal_id |
| `campaigns_settings` | `priority_goal_ids` | Массив приоритетных целей — `arrayJoin` + JOIN `goal_dict` |
| `dm_direct_performance` | COMMENT в колонках | goal_id в комментариях колонок (cart_visits, product_views и др.) |
| `dm_step_goal_impact` | `goal_id`, `goal_name` | Lift по целям на каждом шаге визита |
| `dm_active_clients_scoring` | `recommended_goal_id`, `recommended_goal_name` | Рекомендованная цель для ретаргетинга |
| `socdem_direct_analytics` | `MacroConversions` | Сумма 9 целей (см. docs_socdem_direct_analytics.md) |

## Сценарии использования для AI-агента

### 1. Расшифровка goal_id

**Триггеры**: "Что за цель 31297300?", "Какая цель ID ...?", "Расшифруй goal"

```sql
SELECT goal_id, goal_name, goal_category, goal_trigger
FROM ym_sanok.goal_dict
WHERE goal_id = 31297300  -- подставить нужный goal_id
```

### 2. Расшифровка целей автостратегии кампании

**Триггеры**: "На какую цель оптимизируется кампания?", "Цель стратегии кампании X"

```sql
SELECT
    s.campaign_id,
    s.campaign_name,
    s.strategy_search_type,
    s.strategy_search_goal_id,
    g1.goal_name   AS search_goal_name,
    g1.goal_category AS search_goal_cat,
    s.strategy_network_type,
    s.strategy_network_goal_id,
    g2.goal_name   AS network_goal_name,
    g2.goal_category AS network_goal_cat
FROM ym_sanok.campaigns_settings AS s
LEFT JOIN ym_sanok.goal_dict AS g1 ON s.strategy_search_goal_id = toInt64(g1.goal_id)
LEFT JOIN ym_sanok.goal_dict AS g2 ON s.strategy_network_goal_id = toInt64(g2.goal_id)
WHERE s.state = 'ON'
ORDER BY s.campaign_id
```

### 3. Приоритетные цели кампании с расшифровкой

**Триггеры**: "Какие приоритетные цели у кампании?", "priority_goal_ids расшифровка"

```sql
SELECT
    s.campaign_id,
    s.campaign_name,
    gid AS goal_id,
    g.goal_name,
    g.goal_category,
    gval AS goal_value
FROM ym_sanok.campaigns_settings AS s
ARRAY JOIN
    priority_goal_ids AS gid,
    priority_goal_values AS gval
LEFT JOIN ym_sanok.goal_dict AS g ON gid = toInt64(g.goal_id)
WHERE length(s.priority_goal_ids) > 0
ORDER BY s.campaign_id, gval DESC
```

### 4. Все цели определённой категории

**Триггеры**: "Покажи все цели воронки", "Какие есть транзакционные цели?", "Список автоцелей"

```sql
SELECT goal_id, goal_name, goal_trigger
FROM ym_sanok.goal_dict
WHERE goal_category = 'воронка'  -- транзакция / воронка / звонки / категория / jivo / автоцель / прочее
ORDER BY goal_id
```

### 5. Цели, используемые в скоринге (dm_active_clients_scoring)

**Триггеры**: "Какие цели влияют на скоринг?", "ТОП целей по lift"

```sql
SELECT
    s.goal_id,
    g.goal_name,
    g.goal_category,
    round(s.lift, 1) AS lift,
    s.visit_number
FROM ym_sanok.dm_step_goal_impact AS s
INNER JOIN ym_sanok.goal_dict AS g ON s.goal_id = g.goal_id
WHERE s.snapshot_date = (SELECT max(snapshot_date) FROM ym_sanok.dm_step_goal_impact)
ORDER BY s.lift DESC
LIMIT 20
```

## Технические уточнения

**Типы goal_id**: в `goal_dict` — `UInt64`. В `campaigns_settings` — `Int64` / `Nullable(Int64)`. При JOIN нужен каст: `ON s.strategy_search_goal_id = toInt64(g.goal_id)`.

**Шумовые цели**: goal_id 130300219 («Посетили сайт») — срабатывает на каждом визите. Не использовать для анализа конверсий.

**Дубликаты по смыслу**: несколько целей покрывают один этап воронки (например, 194388760 «Ecommerce: добавление в корзину» и 543662403 «Добавить в корзину»). В `dm_direct_performance` и `dm_active_clients_scoring` используются конкретные goal_id — см. комментарии в тех таблицах.

**Отменённые заказы**: goal_id 543662401 — транзакционная цель «Отмененный заказ». Не путать с успешными транзакциями.

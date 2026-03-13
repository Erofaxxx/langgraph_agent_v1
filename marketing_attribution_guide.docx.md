**МАРКЕТИНГОВАЯ АТРИБУЦИЯ**

Методы анализа, алгоритмы и бюджетная оптимизация

Практическое руководство по построению data-driven системы атрибуции

**СОДЕРЖАНИЕ ДОКУМЕНТА**

| Раздел | Тема | Стр. |
| :---- | :---- | :---- |
| 1 | Что такое атрибуция и зачем она нужна | 2 |
| 2 | Классические модели атрибуции | 3 |
| 3 | Алгоритмические модели (data-driven) | 5 |
| 4 | Структура базы данных для атрибуции | 8 |
| 5 | Алгоритм построения Markov Chain Attribution | 10 |
| 6 | Shapley Value Attribution (теория игр) | 13 |
| 7 | ML-модели: Survival & Multi-touch | 15 |
| 8 | Система оптимизации бюджета | 17 |
| 9 | Пайплайн автоматизации от сырых данных до рекомендаций | 20 |
| 10 | Метрики качества и валидация модели | 23 |

# **1\. Что такое атрибуция и зачем она нужна**

Атрибуция в маркетинге — это процесс присвоения ценности (кредита) каждому маркетинговому каналу или точке контакта в цепочке взаимодействий пользователя с брендом, которая привела к целевому действию (покупке, регистрации, лиду).

**Пример цепочки визитов одного пользователя до покупки:**

|   Organic Search → Paid Social → Email → Direct → ПОКУПКА |
| :---- |
|      (визит 1\)      (визит 2\)   (визит 3\) (визит 4\) |
|  |
|   Вопрос: кому из 4 каналов приписать доход от этой покупки? |

## **Почему атрибуция важна**

Без корректной атрибуции компании систематически недо- или переоценивают отдачу от каналов:

* Last-click переоценивает прямые визиты и branded-поиск

* First-click переоценивает охватные каналы (Display, YouTube)

* Неправильная атрибуция ведёт к субоптимальному распределению бюджета

* Бизнес теряет 20–40% эффективности рекламных расходов

| Проблема | Следствие | Что даёт атрибуция |
| :---- | :---- | :---- |
| Кредит получает последний канал | Переинвестиции в ретаргетинг, брендовый PPC | Справедливое распределение ценности |
| Нет видимости ассист-каналов | Урезаются каналы верхней воронки | Полная картина customer journey |
| Бюджет распределяется интуитивно | Деньги идут не туда | Data-driven бюджетирование |
| Нет понимания синергии каналов | Отдельная оценка каналов — ошибочна | Учёт комбинаторного эффекта |

# **2\. Классические (эвристические) модели атрибуции**

Классические модели применяют заранее заданные правила для распределения кредита. Они просты в реализации, но не используют реальные данные о поведении пользователей.

## **2.1 Last Touch (Last Click)**

Весь кредит получает последнее взаимодействие перед конверсией.

|   Journey: A → B → C → D → КОНВЕРСИЯ |
| :---- |
|   Кредит:  0   0   0   1 |
|  |
|   SQL пример: |
|   SELECT channel, COUNT(\*) as conversions |
|   FROM sessions |
|   WHERE is\_last\_touch \= TRUE AND converted \= TRUE |
|   GROUP BY channel |

Плюсы: простота, совместимость с Google Ads. Минусы: игнорирует всю воронку выше.

## **2.2 First Touch (First Click)**

Весь кредит получает первое взаимодействие.

|   Journey: A → B → C → D → КОНВЕРСИЯ |
| :---- |
|   Кредит:  1   0   0   0 |

Плюсы: оценивает каналы привлечения. Минусы: игнорирует конвертирующие каналы.

## **2.3 Linear (Equal Credit)**

Кредит делится поровну между всеми точками контакта.

|   Journey: A → B → C → D → КОНВЕРСИЯ |
| :---- |
|   Кредит:  0.25  0.25  0.25  0.25 |
|  |
|   def linear\_attribution(touches, conversion\_value): |
|       credit \= conversion\_value / len(touches) |
|       return {touch: credit for touch in touches} |

## **2.4 Time Decay**

Каналы ближе к конверсии получают больший кредит по экспоненциальному убыванию.

|   def time\_decay\_attribution(touches, conversion\_value, half\_life\_days=7): |
| :---- |
|       \# touches \= \[(channel, days\_before\_conversion), ...\] |
|       weights \= \[2 \*\* (-days / half\_life\_days) for \_, days in touches\] |
|       total \= sum(weights) |
|       return {ch: (w/total)\*conversion\_value |
|               for (ch,\_), w in zip(touches, weights)} |

## **2.5 Position-Based (U-Shaped / W-Shaped)**

U-Shaped: 40% первому, 40% последнему, 20% остальным поровну.

|   def u\_shaped(touches, value): |
| :---- |
|       n \= len(touches) |
|       if n \== 1: return {touches\[0\]: value} |
|       if n \== 2: return {touches\[0\]: value\*0.5, touches\[1\]: value\*0.5} |
|       credits \= {} |
|       credits\[touches\[0\]\]  \= value \* 0.40 |
|       credits\[touches\[-1\]\] \= value \* 0.40 |
|       middle\_share \= value \* 0.20 / (n \- 2\) |
|       for t in touches\[1:-1\]: |
|           credits\[t\] \= middle\_share |
|       return credits |

| Модель | Лучший use-case | Ограничение |
| :---- | :---- | :---- |
| Last Touch | E-commerce с короткой воронкой | Игнорирует awareness-каналы |
| First Touch | B2B, long sales cycle (attribution discovery) | Игнорирует нижнюю воронку |
| Linear | Когда нет данных для data-driven | Нет дифференциации важности |
| Time Decay | Промо с ограниченным сроком | Занижает awareness |
| U-Shaped | Стандартная маркетинговая воронка | Произвольные веса 40/20/40 |
| W-Shaped | 3 ключевые точки (first/lead/close) | Не масштабируется |

# **3\. Алгоритмические (data-driven) модели атрибуции**

В отличие от эвристических, data-driven модели вычисляют веса атрибуции из реальных данных о поведении пользователей. Это принципиально меняет качество решений.

| Три ключевых класса data-driven алгоритмов |
| :---- |
| 1\. Markov Chain Attribution — граф переходов между каналами, removal effect |
| 2\. Shapley Value Attribution — теория кооперативных игр, справедливое распределение |
| 3\. ML-модели — Logistic Regression, Survival Analysis, LSTM, Causal Inference |

## **Почему data-driven лучше эвристики**

* Учитывает реальные пути конверсии из вашей базы данных

* Автоматически адаптируется при изменении mix-а каналов

* Количественно измеряет incremental value каждого канала

* Учитывает синергию: канал A+B вместе работает лучше, чем по отдельности

* Можно валидировать holdout-тестами и A/B экспериментами

# **4\. Структура базы данных для атрибуции**

Прежде чем строить модель, необходимо правильно организовать сырые данные. Минимальные требования к схеме данных:

## **4.1 Таблица сессий (sessions / visits)**

|   CREATE TABLE sessions ( |
| :---- |
|     session\_id      UUID PRIMARY KEY, |
|     user\_id         VARCHAR(64),     \-- анонимный или авторизованный |
|     anonymous\_id    VARCHAR(64),     \-- cookie / device fingerprint |
|     channel         VARCHAR(64),     \-- 'paid\_search', 'organic', 'email', ... |
|     source          VARCHAR(128),    \-- utm\_source |
|     medium          VARCHAR(64),     \-- utm\_medium |
|     campaign        VARCHAR(128),    \-- utm\_campaign |
|     landing\_page    TEXT, |
|     session\_start   TIMESTAMP, |
|     session\_end     TIMESTAMP, |
|     pages\_viewed    INTEGER, |
|     created\_at      TIMESTAMP DEFAULT NOW() |
|   ); |

## **4.2 Таблица конверсий (conversions / purchases)**

|   CREATE TABLE conversions ( |
| :---- |
|     conversion\_id   UUID PRIMARY KEY, |
|     user\_id         VARCHAR(64), |
|     anonymous\_id    VARCHAR(64), |
|     session\_id      UUID REFERENCES sessions(session\_id), |
|     order\_id        VARCHAR(128), |
|     revenue         DECIMAL(12,2), |
|     converted\_at    TIMESTAMP, |
|     product\_type    VARCHAR(64), |
|     is\_new\_customer BOOLEAN |
|   ); |

## **4.3 SQL: Построение customer journeys**

Ключевой запрос — собрать цепочки визитов для каждого пользователя до конверсии:

|   WITH user\_journeys AS ( |
| :---- |
|     SELECT |
|       s.user\_id, |
|       s.channel, |
|       s.session\_start, |
|       c.converted\_at, |
|       c.revenue, |
|       c.conversion\_id, |
|       ROW\_NUMBER() OVER ( |
|         PARTITION BY s.user\_id, c.conversion\_id |
|         ORDER BY s.session\_start |
|       ) as touch\_number, |
|       COUNT(\*) OVER ( |
|         PARTITION BY s.user\_id, c.conversion\_id |
|       ) as total\_touches |
|     FROM sessions s |
|     JOIN conversions c |
|       ON s.user\_id \= c.user\_id |
|       AND s.session\_start \<= c.converted\_at |
|       AND s.session\_start \>= c.converted\_at \- INTERVAL '90 days' |
|   ) |
|   SELECT |
|     conversion\_id, |
|     user\_id, |
|     revenue, |
|     ARRAY\_AGG(channel ORDER BY touch\_number) as journey, |
|     total\_touches |
|   FROM user\_journeys |
|   GROUP BY conversion\_id, user\_id, revenue, total\_touches; |

**Результат — таблица с journey-путями вида:**

| conversion\_id | user\_id | journey | revenue |
| :---- | :---- | :---- | :---- |
| conv\_001 | usr\_123 | \[organic, paid\_social, email\] | 4500 руб |
| conv\_002 | usr\_456 | \[paid\_search, direct\] | 1200 руб |
| conv\_003 | usr\_789 | \[display, organic, paid\_search, email, direct\] | 8900 руб |

# **5\. Алгоритм: Markov Chain Attribution**

Markov Chain Attribution — наиболее популярный data-driven алгоритм. Моделирует переходы пользователя между каналами как граф вероятностей, затем оценивает incremental contribution каждого канала через "removal effect".

## **5.1 Концепция**

* Строится направленный граф: узлы \= каналы \+ START \+ CONVERSION \+ NULL

* Рёбра \= вероятности переходов P(A→B) \= count(A→B) / count(A→\*)

* Removal Effect: насколько падает вероятность конверсии, если убрать канал X

* Attribution Credit \= Removal Effect / сумма всех Removal Effects

## **5.2 Полный Python-алгоритм**

|   import pandas as pd |
| :---- |
|   import numpy as np |
|   from collections import defaultdict |
|   from itertools import combinations |
|  |
|   def build\_transition\_matrix(journeys): |
|       """ |
|       journeys: list of lists, e.g. \[\['organic','email','direct'\], ...\] |
|       Returns: dict of {from\_state: {to\_state: probability}} |
|       """ |
|       counts \= defaultdict(lambda: defaultdict(int)) |
|  |
|       for journey in journeys: |
|           \# Добавляем START и CONVERSION |
|           full\_path \= \['START'\] \+ journey \+ \['CONVERSION'\] |
|           for i in range(len(full\_path) \- 1): |
|               counts\[full\_path\[i\]\]\[full\_path\[i+1\]\] \+= 1 |
|  |
|       \# Нормализуем в вероятности |
|       transitions \= {} |
|       for from\_state, to\_states in counts.items(): |
|           total \= sum(to\_states.values()) |
|           transitions\[from\_state\] \= { |
|               to: cnt/total for to, cnt in to\_states.items() |
|           } |
|       return transitions |
|  |
|   def conversion\_probability(transitions, max\_steps=100): |
|       """Вычисляет P(CONVERSION) через случайное блуждание""" |
|       \# Используем аналитическое решение через матрицу переходов |
|       states \= list(set( |
|           list(transitions.keys()) \+ |
|           \[s for d in transitions.values() for s in d.keys()\] |
|       )) |
|       \# Исключаем absorbing states |
|       transient \= \[s for s in states |
|                    if s not in ('CONVERSION', 'NULL')\] |
|  |
|       n \= len(transient) |
|       state\_idx \= {s: i for i, s in enumerate(transient)} |
|  |
|       \# Q матрица (transient → transient переходы) |
|       Q \= np.zeros((n, n)) |
|       R\_conv \= np.zeros(n)  \# вектор поглощения в CONVERSION |
|  |
|       for s in transient: |
|           i \= state\_idx\[s\] |
|           for to, prob in transitions.get(s, {}).items(): |
|               if to in state\_idx: |
|                   Q\[i, state\_idx\[to\]\] \= prob |
|               elif to \== 'CONVERSION': |
|                   R\_conv\[i\] \= prob |
|  |
|       \# Fundamental matrix: N \= (I \- Q)^{-1} |
|       I \= np.eye(n) |
|       try: |
|           N \= np.linalg.inv(I \- Q) |
|       except np.linalg.LinAlgError: |
|           \# Fallback: pseudo-inverse |
|           N \= np.linalg.pinv(I \- Q) |
|  |
|       \# P(conversion | start from START) |
|       B \= N @ R\_conv  \# absorption probabilities |
|       start\_idx \= state\_idx.get('START') |
|       if start\_idx is not None: |
|           return B\[start\_idx\], B, state\_idx |
|       return 0, B, state\_idx |
|  |
|   def markov\_attribution(journeys): |
|       """ |
|       Основная функция: возвращает dict {channel: attribution\_credit} |
|       Кредиты нормализованы, сумма \= 1.0 |
|       """ |
|       transitions \= build\_transition\_matrix(journeys) |
|       base\_prob, \_, \_ \= conversion\_probability(transitions) |
|  |
|       \# Все каналы кроме START/CONVERSION/NULL |
|       channels \= set() |
|       for journey in journeys: |
|           channels.update(journey) |
|  |
|       removal\_effects \= {} |
|  |
|       for channel in channels: |
|           \# Убираем канал из графа переходов |
|           modified \= {} |
|           for from\_s, to\_dict in transitions.items(): |
|               if from\_s \== channel: |
|                   \# Этот узел удалён — переходы из него идут в NULL |
|                   modified\[from\_s\] \= {'NULL': 1.0} |
|                   continue |
|               new\_to \= {} |
|               for to\_s, prob in to\_dict.items(): |
|                   if to\_s \== channel: |
|                       \# Перераспределяем вероятность на NULL |
|                       new\_to\['NULL'\] \= new\_to.get('NULL', 0\) \+ prob |
|                   else: |
|                       new\_to\[to\_s\] \= prob |
|               modified\[from\_s\] \= new\_to |
|  |
|           removed\_prob, \_, \_ \= conversion\_probability(modified) |
|           removal\_effects\[channel\] \= max(0, base\_prob \- removed\_prob) |
|  |
|       \# Нормализация |
|       total\_re \= sum(removal\_effects.values()) |
|       if total\_re \== 0: |
|           n \= len(channels) |
|           return {ch: 1/n for ch in channels} |
|  |
|       credits \= {ch: re/total\_re for ch, re in removal\_effects.items()} |
|       return credits |
|  |
|   \# \=== ИСПОЛЬЗОВАНИЕ \=== |
|   journeys \= \[ |
|       \['organic', 'paid\_social', 'email'\], |
|       \['paid\_search', 'direct'\], |
|       \['paid\_social', 'organic', 'email', 'direct'\], |
|       \['organic', 'direct'\], |
|       \['email', 'direct'\], |
|   \] |
|  |
|   credits \= markov\_attribution(journeys) |
|   for channel, credit in sorted(credits.items(), |
|                                 key=lambda x: \-x\[1\]): |
|       print(f'{channel:20s}: {credit:.1%}') |

# **6\. Алгоритм: Shapley Value Attribution**

Shapley Value из теории кооперативных игр — математически обоснованный метод справедливого распределения «выигрыша» (конверсии) между «игроками» (каналами). Это единственный метод, удовлетворяющий четырём аксиомам справедливости одновременно.

## **6.1 Аксиомы Шепли (почему это «справедливо»)**

* Эффективность: сумма кредитов \= полная ценность конверсии

* Симметрия: каналы с одинаковым вкладом получают одинаковый кредит

* Нулевой игрок: канал без вклада получает ноль

* Аддитивность: кредиты складываются линейно

## **6.2 Формула и алгоритм**

|   \# Shapley value для канала i: |
| :---- |
|   \# φ\_i \= Σ \[ |S|\!(n-|S|-1)\!/n\! \] \* \[v(S∪{i}) \- v(S)\] |
|   \# где S — все подмножества без канала i |
|   \# v(S) — конверсионная ценность коалиции S |
|  |
|   from itertools import combinations |
|   from math import factorial |
|  |
|   def compute\_coalition\_values(journeys): |
|       """ |
|       Для каждого подмножества каналов считаем: |
|       conversion\_rate \= конверсии / (конверсии \+ нет конверсии) |
|       через journeys из БД |
|       """ |
|       channel\_sets \= defaultdict(lambda: {'conv': 0, 'total': 0}) |
|  |
|       for journey, converted in journeys:  \# (list\_of\_channels, bool) |
|           key \= frozenset(journey) |
|           channel\_sets\[key\]\['total'\] \+= 1 |
|           if converted: |
|               channel\_sets\[key\]\['conv'\] \+= 1 |
|  |
|       \# v(S) \= взвешенная сумма conversion rates подмножеств S |
|       coalition\_values \= {} |
|       for subset, stats in channel\_sets.items(): |
|           coalition\_values\[subset\] \= ( |
|               stats\['conv'\] / stats\['total'\] |
|               if stats\['total'\] \> 0 else 0 |
|           ) |
|       return coalition\_values |
|  |
|   def shapley\_attribution(journeys\_with\_outcomes, all\_channels): |
|       """ |
|       journeys\_with\_outcomes: \[(channels\_list, converted\_bool), ...\] |
|       all\_channels: list of all channel names |
|       """ |
|       n \= len(all\_channels) |
|       v \= compute\_coalition\_values(journeys\_with\_outcomes) |
|  |
|       def coalition\_value(S): |
|           key \= frozenset(S) |
|           \# Если точного совпадения нет — берём среднее похожих коалиций |
|           if key in v: |
|               return v\[key\] |
|           \# Fallback: linear interpolation через подмножества |
|           subset\_vals \= \[ |
|               v\[k\] for k in v |
|               if k.issubset(key) and len(k) \>= len(key) \- 1 |
|           \] |
|           return np.mean(subset\_vals) if subset\_vals else 0 |
|  |
|       shapley\_values \= {} |
|  |
|       for i, channel in enumerate(all\_channels): |
|           others \= \[c for c in all\_channels if c \!= channel\] |
|           phi \= 0 |
|  |
|           for size in range(len(others) \+ 1): |
|               weight \= (factorial(size) \* factorial(n \- size \- 1\) |
|                         / factorial(n)) |
|               for subset in combinations(others, size): |
|                   S \= list(subset) |
|                   marginal \= (coalition\_value(S \+ \[channel\]) |
|                               \- coalition\_value(S)) |
|                   phi \+= weight \* marginal |
|  |
|           shapley\_values\[channel\] \= phi |
|  |
|       \# Нормализация в проценты |
|       total \= sum(max(0, v) for v in shapley\_values.values()) |
|       return {ch: max(0, val)/total if total \> 0 else 1/n |
|               for ch, val in shapley\_values.items()} |

**Примечание по производительности:**

Точный алгоритм Шепли имеет сложность O(2^n), что проблематично при n \> 15 каналов. Для больших наборов используется Monte Carlo Shapley:

|   def monte\_carlo\_shapley(journeys\_with\_outcomes, channels, |
| :---- |
|                           n\_samples=10000): |
|       """Аппроксимация Шепли через случайные перестановки""" |
|       v \= compute\_coalition\_values(journeys\_with\_outcomes) |
|       marginals \= defaultdict(list) |
|  |
|       for \_ in range(n\_samples): |
|           perm \= np.random.permutation(channels) |
|           coalition \= \[\] |
|           prev\_val \= 0 |
|           for ch in perm: |
|               coalition.append(ch) |
|               curr\_val \= v.get(frozenset(coalition), 0\) |
|               marginals\[ch\].append(curr\_val \- prev\_val) |
|               prev\_val \= curr\_val |
|  |
|       shapley \= {ch: np.mean(vals) |
|                  for ch, vals in marginals.items()} |
|       total \= sum(max(0, v) for v in shapley.values()) |
|       return {ch: max(0, val)/total if total \> 0 else 1/len(channels) |
|               for ch, val in shapley.items()} |

# **7\. ML-модели для атрибуции**

## **7.1 Logistic Regression с channel presence features**

Самый быстрый подход: для каждой конверсии создаём бинарные фичи по наличию каналов, обучаем логрег, коэффициенты \= attribution weights.

|   import pandas as pd |
| :---- |
|   from sklearn.linear\_model import LogisticRegression |
|   from sklearn.preprocessing import StandardScaler |
|  |
|   def lr\_attribution(df\_journeys): |
|       """ |
|       df\_journeys: DataFrame с колонками: |
|         \- journey (list of channels) |
|         \- converted (0/1) |
|       """ |
|       \# Все уникальные каналы |
|       all\_channels \= set( |
|           ch for journey in df\_journeys\['journey'\] for ch in journey |
|       ) |
|  |
|       \# One-hot encoding: присутствует ли канал в journey |
|       for ch in all\_channels: |
|           df\_journeys\[f'has\_{ch}'\] \= df\_journeys\['journey'\].apply( |
|               lambda j: int(ch in j) |
|           ) |
|  |
|       feature\_cols \= \[f'has\_{ch}' for ch in all\_channels\] |
|       X \= df\_journeys\[feature\_cols\].values |
|       y \= df\_journeys\['converted'\].values |
|  |
|       model \= LogisticRegression(C=1.0, max\_iter=1000) |
|       model.fit(X, y) |
|  |
|       \# Коэффициенты как прокси attribution weights |
|       coefficients \= dict(zip(all\_channels, model.coef\_\[0\])) |
|  |
|       \# Нормализация положительных коэффициентов |
|       pos\_coefs \= {ch: max(0, c) for ch, c in coefficients.items()} |
|       total \= sum(pos\_coefs.values()) |
|       return {ch: v/total if total \> 0 else 0 |
|               for ch, v in pos\_coefs.items()} |

## **7.2 Survival Analysis (Time-to-Conversion)**

Модель оценивает, как каждый канал влияет на скорость конверсии — особенно полезна для B2B с длинным циклом продаж.

|   from lifelines import CoxPHFitter |
| :---- |
|   import pandas as pd |
|  |
|   def survival\_attribution(df): |
|       """ |
|       df: DataFrame со строками-пользователями: |
|         \- duration: дни до конверсии (или до конца наблюдения) |
|         \- event: 1=конвертировался, 0=цензурирован |
|         \- ch\_\*: бинарные фичи по каналам |
|       """ |
|       cph \= CoxPHFitter() |
|       channel\_cols \= \[c for c in df.columns if c.startswith('ch\_')\] |
|  |
|       cph.fit(df\[channel\_cols \+ \['duration', 'event'\]\], |
|               duration\_col='duration', |
|               event\_col='event') |
|  |
|       \# Hazard ratios: exp(coef) \> 1 \= ускоряет конверсию |
|       hazard\_ratios \= np.exp(cph.params\_) |
|  |
|       \# Нормализуем в attribution weights |
|       pos \= {ch.replace('ch\_',''): max(0, hr \- 1\) |
|              for ch, hr in hazard\_ratios.items()} |
|       total \= sum(pos.values()) |
|       return {ch: v/total for ch, v in pos.items()} if total \> 0 else pos |

## **7.3 Выбор алгоритма: сравнительная таблица**

| Алгоритм | Интерпретируемость | Точность | Сложность | Мин. данных |
| :---- | :---- | :---- | :---- | :---- |
| Last/First Click | Максимальная | Низкая | Минимальная | 100+ конв. |
| Linear / Time Decay | Высокая | Низкая-средняя | Низкая | 100+ конв. |
| Markov Chain | Высокая | Высокая | Средняя | 1000+ конв. |
| Shapley Value | Высокая (аксиомы) | Высокая | Высокая (2^n) | 2000+ конв. |
| Logistic Regression | Средняя | Средняя | Низкая | 500+ конв. |
| Survival Analysis | Средняя | Высокая (time) | Средняя | 1000+ конв. |
| LSTM / Deep | Низкая | Наивысшая | Очень высокая | 50k+ конв. |

# **8\. Система оптимизации бюджета на основе атрибуции**

Attribution — это диагностика. Оптимизация бюджета — это лечение. На основе attribution credits строим модель максимизации ROI.

## **8.1 Расчёт ROAS по модели атрибуции**

|   def calculate\_attributed\_roas( |
| :---- |
|           attribution\_credits,  \# {channel: credit\_share} |
|           total\_revenue,        \# общая выручка за период |
|           channel\_costs         \# {channel: spend} |
|   ): |
|       """ |
|       Возвращает attributed ROAS для каждого канала |
|       """ |
|       attributed\_revenue \= { |
|           ch: credit \* total\_revenue |
|           for ch, credit in attribution\_credits.items() |
|       } |
|  |
|       roas \= {} |
|       for channel in attribution\_credits: |
|           spend \= channel\_costs.get(channel, 0\) |
|           if spend \> 0: |
|               roas\[channel\] \= attributed\_revenue\[channel\] / spend |
|           else: |
|               roas\[channel\] \= float('inf')  \# органика |
|  |
|       return roas, attributed\_revenue |

## **8.2 Оптимизация бюджета: Linear Programming**

Задача: максимизировать суммарный attributed ROAS при ограничениях на бюджет.

|   from scipy.optimize import linprog, minimize |
| :---- |
|   import numpy as np |
|  |
|   def optimize\_budget\_simple(roas\_by\_channel, total\_budget, |
|                               min\_per\_channel=0.05, |
|                               max\_per\_channel=0.60): |
|       """ |
|       Простая аллокация: пропорционально ROAS с ограничениями |
|       roas\_by\_channel: {channel: roas\_value} |
|       total\_budget: общий бюджет в рублях |
|       min/max: ограничения доли бюджета на канал |
|       """ |
|       channels \= \[ch for ch, r in roas\_by\_channel.items() |
|                   if r \< float('inf') and r \> 0\] |
|       n \= len(channels) |
|  |
|       roas\_vals \= np.array(\[roas\_by\_channel\[ch\] for ch in channels\]) |
|  |
|       \# Нормализованное ROAS \-\> базовые веса |
|       base\_weights \= roas\_vals / roas\_vals.sum() |
|  |
|       \# Применяем ограничения min/max |
|       weights \= np.clip(base\_weights, min\_per\_channel, max\_per\_channel) |
|       weights /= weights.sum()  \# renormalize |
|  |
|       return {ch: w \* total\_budget |
|               for ch, w in zip(channels, weights)} |
|  |
|  |
|   def optimize\_budget\_with\_saturation( |
|           spend\_response\_curves,  \# {ch: функция spend-\>revenue} |
|           total\_budget, |
|           bounds\_per\_channel      \# {ch: (min\_spend, max\_spend)} |
|   ): |
|       """ |
|       Продвинутая оптимизация с кривыми насыщения (diminishing returns) |
|       Использует scipy.optimize.minimize |
|       """ |
|       channels \= list(spend\_response\_curves.keys()) |
|       n \= len(channels) |
|  |
|       def total\_revenue(x): |
|           \# x \= вектор расходов по каналам |
|           return \-sum(spend\_response\_curves\[ch\](x\[i\]) |
|                      for i, ch in enumerate(channels)) |
|  |
|       \# Ограничение: сумма расходов \= total\_budget |
|       constraints \= \[{'type': 'eq', |
|                        'fun': lambda x: sum(x) \- total\_budget}\] |
|  |
|       \# Границы для каждого канала |
|       bounds \= \[bounds\_per\_channel.get(ch, (0, total\_budget)) |
|                 for ch in channels\] |
|  |
|       x0 \= \[total\_budget / n\] \* n  \# начальное равномерное распределение |
|  |
|       result \= minimize(total\_revenue, x0, |
|                         method='SLSQP', |
|                         bounds=bounds, |
|                         constraints=constraints) |
|  |
|       if result.success: |
|           return dict(zip(channels, result.x)) |
|       else: |
|           return optimize\_budget\_simple(  \# fallback |
|               {ch: 1.0 for ch in channels}, total\_budget |
|           ) |

## **8.3 Кривые насыщения (Diminishing Returns)**

Каждый канал имеет точку насыщения — дополнительные вложения дают всё меньший прирост. Моделируем через Hill function:

|   def hill\_curve(spend, k, n, max\_revenue): |
| :---- |
|       """ |
|       Hill function: часто используется в Media Mix Modeling |
|       k \= half-saturation point (расход, при котором 50% max\_revenue) |
|       n \= коэффициент крутизны (обычно 1-3) |
|       """ |
|       return max\_revenue \* (spend\*\*n) / (k\*\*n \+ spend\*\*n) |
|  |
|   def fit\_saturation\_curve(spend\_history, revenue\_history): |
|       """Фитируем Hill function по историческим данным канала""" |
|       from scipy.optimize import curve\_fit |
|  |
|       def hill(spend, k, n, max\_rev): |
|           return max\_rev \* (spend\*\*n) / (k\*\*n \+ spend\*\*n) |
|  |
|       popt, \_ \= curve\_fit(hill, spend\_history, revenue\_history, |
|                            p0=\[np.median(spend\_history), 2, |
|                                max(revenue\_history)\], |
|                            bounds=(0, np.inf), |
|                            maxfev=5000) |
|       k\_fit, n\_fit, max\_rev\_fit \= popt |
|  |
|       return lambda s: hill(s, k\_fit, n\_fit, max\_rev\_fit) |

# **9\. Полный пайплайн: от сырых данных до рекомендаций**

Архитектура end-to-end системы атрибуции, которую можно развернуть на реальной БД:

|   ┌─────────────────────────────────────────────────────────────┐ |
| :---- |
|   │                    ATTRIBUTION PIPELINE                     │ |
|   ├─────────────────────────────────────────────────────────────┤ |
|   │  1\. DATA LAYER (PostgreSQL / BigQuery / ClickHouse)         │ |
|   │     sessions \+ conversions \+ costs                          │ |
|   │                    ↓                                        │ |
|   │  2\. JOURNEY BUILDER (SQL \+ Python)                          │ |
|   │     user\_id → channel sequence → conversion                 │ |
|   │                    ↓                                        │ |
|   │  3\. ATTRIBUTION ENGINE (Python)                             │ |
|   │     Markov / Shapley / ML → channel credits                 │ |
|   │                    ↓                                        │ |
|   │  4\. PERFORMANCE CALCULATOR                                  │ |
|   │     attributed ROAS, CPA, iROAS по каналу                   │ |
|   │                    ↓                                        │ |
|   │  5\. BUDGET OPTIMIZER                                        │ |
|   │     saturation curves \+ LP → optimal allocation             │ |
|   │                    ↓                                        │ |
|   │  6\. REPORTING LAYER (BI tool / API)                         │ |
|   │     рекомендации \+ мониторинг                               │ |
|   └─────────────────────────────────────────────────────────────┘ |

## **9.1 Полный Python-класс AttributionPipeline**

|   class AttributionPipeline: |
| :---- |
|  |
|       def \_\_init\_\_(self, db\_conn, lookback\_days=90, |
|                    model='markov'): |
|           self.db \= db\_conn |
|           self.lookback\_days \= lookback\_days |
|           self.model \= model |
|  |
|       def extract\_journeys(self): |
|           """Извлекаем journey из БД""" |
|           query \= ''' |
|               WITH touches AS ( |
|                 SELECT |
|                   c.conversion\_id, |
|                   c.revenue, |
|                   s.channel, |
|                   s.session\_start, |
|                   c.converted\_at |
|                 FROM conversions c |
|                 JOIN sessions s ON s.user\_id \= c.user\_id |
|                 WHERE s.session\_start BETWEEN |
|                   c.converted\_at \- INTERVAL '%s days' |
|                   AND c.converted\_at |
|                   AND c.converted\_at \>= NOW() \- INTERVAL '30 days' |
|               ) |
|               SELECT |
|                 conversion\_id, |
|                 revenue, |
|                 ARRAY\_AGG(channel ORDER BY session\_start) as journey |
|               FROM touches |
|               GROUP BY conversion\_id, revenue |
|           ''' % self.lookback\_days |
|  |
|           return pd.read\_sql(query, self.db) |
|  |
|       def run\_attribution(self, df\_journeys): |
|           journeys \= df\_journeys\['journey'\].tolist() |
|           if self.model \== 'markov': |
|               return markov\_attribution(journeys) |
|           elif self.model \== 'shapley': |
|               all\_ch \= list(set(ch for j in journeys for ch in j)) |
|               jwo \= \[(j, True) for j in journeys\] |
|               return monte\_carlo\_shapley(jwo, all\_ch) |
|           elif self.model \== 'linear': |
|               return linear\_attribution\_aggregate(journeys) |
|           else: |
|               raise ValueError(f'Unknown model: {self.model}') |
|  |
|       def calculate\_performance(self, credits, |
|                                  df\_journeys, costs): |
|           total\_rev \= df\_journeys\['revenue'\].sum() |
|           roas, attr\_rev \= calculate\_attributed\_roas( |
|               credits, total\_rev, costs |
|           ) |
|           return pd.DataFrame({ |
|               'channel': list(credits.keys()), |
|               'attribution\_credit': list(credits.values()), |
|               'attributed\_revenue': \[attr\_rev.get(ch,0) |
|                                      for ch in credits\], |
|               'spend': \[costs.get(ch, 0\) for ch in credits\], |
|               'attributed\_roas': \[roas.get(ch, 0\) |
|                                   for ch in credits\], |
|           }).sort\_values('attributed\_roas', ascending=False) |
|  |
|       def recommend\_budget(self, performance\_df, |
|                             total\_budget): |
|           roas\_dict \= dict(zip( |
|               performance\_df\['channel'\], |
|               performance\_df\['attributed\_roas'\] |
|           )) |
|           allocation \= optimize\_budget\_simple( |
|               roas\_dict, total\_budget |
|           ) |
|           perf \= performance\_df.copy() |
|           perf\['recommended\_budget'\] \= perf\['channel'\].map(allocation) |
|           perf\['budget\_change'\] \= ( |
|               perf\['recommended\_budget'\] \- perf\['spend'\] |
|           ) |
|           perf\['budget\_change\_pct'\] \= ( |
|               perf\['budget\_change'\] / perf\['spend'\] \* 100 |
|           ).round(1) |
|           return perf |
|  |
|       def run(self, costs, total\_budget): |
|           print('1. Extracting journeys...') |
|           df \= self.extract\_journeys() |
|           print(f'   Found {len(df)} conversions') |
|  |
|           print('2. Running attribution model...') |
|           credits \= self.run\_attribution(df) |
|  |
|           print('3. Calculating performance...') |
|           perf \= self.calculate\_performance(credits, df, costs) |
|  |
|           print('4. Generating budget recommendations...') |
|           recommendations \= self.recommend\_budget(perf, total\_budget) |
|  |
|           return recommendations |

# **10\. Метрики качества и валидация модели**

## **10.1 Как оценить качество модели атрибуции**

Атрибуцию сложно валидировать напрямую, поскольку нельзя одновременно наблюдать мир с и без канала. Используем косвенные методы:

| Метод | Описание | Когда применять |
| :---- | :---- | :---- |
| Holdout Test | Отключаем канал для 10-20% пользователей, сравниваем конверсии | Есть контроль над показами |
| Geo Holdout | Отключаем канал в отдельных регионах/городах | Каналы с гео-таргетингом |
| Time Series Check | Attribution credit vs реальный spend — корреляция во времени | Ретроспективный анализ |
| Cross-validation | Train/test split по времени — качество прогнозов | Предиктивные ML-модели |
| ROAS Consistency | Атрибутированный ROAS ≈ реальному Incrementality ROAS | Сравнение с MMM |

## **10.2 Система мониторинга и обновления**

|   class AttributionMonitor: |
| :---- |
|  |
|       def check\_data\_quality(self, df\_journeys): |
|           issues \= \[\] |
|  |
|           \# 1\. Минимум данных |
|           if len(df\_journeys) \< 500: |
|               issues.append('WARNING: Less than 500 conversions' |
|                             ' — model may be unreliable') |
|  |
|           \# 2\. Покрытие каналов |
|           single\_touch \= (df\_journeys\['journey'\] |
|                           .apply(len) \== 1).mean() |
|           if single\_touch \> 0.8: |
|               issues.append(f'WARNING: {single\_touch:.0%} single-touch' |
|                             ' journeys — check tracking') |
|  |
|           \# 3\. Распределение длин journey |
|           avg\_len \= df\_journeys\['journey'\].apply(len).mean() |
|           if avg\_len \< 1.5: |
|               issues.append('WARNING: Very short journeys' |
|                             ' — attribution window may be too small') |
|  |
|           \# 4\. Свежесть данных |
|           last\_conv \= df\_journeys\['converted\_at'\].max() |
|           days\_lag \= (pd.Timestamp.now() \- last\_conv).days |
|           if days\_lag \> 3: |
|               issues.append(f'WARNING: Last conversion {days\_lag} days ago' |
|                             ' — check data pipeline') |
|  |
|           return issues |
|  |
|       def detect\_attribution\_shift(self, credits\_old, credits\_new, |
|                                     threshold=0.15): |
|           """Детектируем значительные изменения атрибуции""" |
|           alerts \= \[\] |
|           for channel in set(list(credits\_old) \+ list(credits\_new)): |
|               old \= credits\_old.get(channel, 0\) |
|               new \= credits\_new.get(channel, 0\) |
|               if abs(new \- old) \> threshold: |
|                   direction \= 'up' if new \> old else 'down' |
|                   alerts.append({ |
|                       'channel': channel, |
|                       'old\_credit': f'{old:.1%}', |
|                       'new\_credit': f'{new:.1%}', |
|                       'direction': direction, |
|                   }) |
|           return alerts |

## **10.3 Пример выходной таблицы рекомендаций**

| Канал | Attribution Credit | Attributed ROAS | Текущий бюджет | Рек. бюджет | Изменение |
| :---- | :---- | :---- | :---- | :---- | :---- |
| Paid Search (Brand) | 8% | 1.8x | 250 000 ₽ | 140 000 ₽ | \-44% |
| Paid Search (Non-brand) | 22% | 4.1x | 200 000 ₽ | 280 000 ₽ | \+40% |
| Paid Social (FB/VK) | 18% | 3.2x | 180 000 ₽ | 220 000 ₽ | \+22% |
| Email Marketing | 15% | 8.7x | 30 000 ₽ | 50 000 ₽ | \+67% |
| Organic / SEO | 24% | ∞ | 80 000 ₽ | 120 000 ₽ | \+50% |
| Display / Programmatic | 7% | 1.1x | 160 000 ₽ | 60 000 ₽ | \-63% |
| YouTube | 6% | 1.9x | 100 000 ₽ | 80 000 ₽ | \-20% |

| Ключевые выводы из примера |
| :---- |
| Email Marketing (ROAS 8.7x) — хронически недофинансирован, \+67% бюджета |
| Non-brand Search и SEO — главные конвертеры, требуют инвестиций |
| Display Programmatic (ROAS 1.1x) — почти не окупается в атрибуционной модели |
| Brand Search переоценён Last-click моделью; data-driven показывает ROAS 1.8x |
| Синергия Email \+ Organic создаёт 39% всей атрибутированной выручки |

## **10.4 Расписание обновления модели**

| Частота | Действие | Триггер |
| :---- | :---- | :---- |
| Ежедневно | Обновление journey-данных и attribution credits | Cron job |
| Еженедельно | Пересчёт ROAS и бюджетных рекомендаций | Cron job |
| Ежемесячно | Ре-обучение ML-модели / пересчёт Shapley | Cron или Manual |
| Ежеквартально | Валидация через holdout-тесты, ревью окна атрибуции | Manual review |

**Attribution без оптимизации — диагноз без лечения.** Начните с Markov Chain на данных 90 дней → получите attribution credits → рассчитайте ROAS → перераспределите 10-15% бюджета → измерьте результат.
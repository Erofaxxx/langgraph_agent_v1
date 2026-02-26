# Инструкция по установке на Ubuntu Server

## Требования

- Ubuntu 22.04 / 24.04 LTS
- Доменное имя `server.asktab.ru`, указывающее на IP сервера (A-запись в DNS)
- Порты 80 и 443 открыты в firewall
- Доступ к ClickHouse (Яндекс Cloud или self-hosted)
- API ключ OpenRouter (`sk-or-v1-...`)

---

## Шаг 1. Подключение к серверу и клонирование репозитория

```bash
ssh root@server.asktab.ru

# Клонируем репозиторий
git clone https://github.com/Erofaxxx/langgraph_agent_v1.git /root/repo

# Переходим в папку агента
cp -r /root/repo/clickhouse_analytics_agent /root/clickhouse_analytics_agent
cd /root/clickhouse_analytics_agent
```

---

## Шаг 2. Установка системных зависимостей

```bash
apt-get update
apt-get install -y python3 python3-pip python3-venv nginx certbot python3-certbot-nginx curl wget
```

---

## Шаг 3. Python окружение и зависимости

```bash
cd /root/clickhouse_analytics_agent

# Создаём виртуальное окружение
python3 -m venv venv

# Активируем
source venv/bin/activate

# Обновляем pip
pip install --upgrade pip

# Устанавливаем зависимости
pip install -r requirements.txt
```

Проверка установки:
```bash
python -c "import langgraph, langchain_openai, clickhouse_connect, pandas, matplotlib; print('OK')"
```

---

## Шаг 4. Скачивание SSL сертификата Яндекс Cloud (если используете Яндекс ClickHouse)

```bash
cd /root/clickhouse_analytics_agent
curl https://storage.yandexcloud.net/cloud-certs/CA.pem -o YandexInternalRootCA.crt
```

Если у вас другой ClickHouse (self-hosted без SSL), в `.env` оставьте `CLICKHOUSE_SSL_CERT_PATH=` пустым
и установите порт `CLICKHOUSE_PORT=8123` (HTTP, без SSL).

---

## Шаг 5. Настройка переменных окружения

```bash
cp .env.example .env
nano .env
```

Заполните обязательные поля:

```env
# Ваш ключ OpenRouter (https://openrouter.ai/keys)
OPENROUTER_API_KEY=sk-or-v1-XXXXXXXXXXXXXXXX

# ClickHouse — Яндекс Cloud
CLICKHOUSE_HOST=your-cluster.mdb.yandexcloud.net
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=your_user
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=your_database
CLICKHOUSE_SSL_CERT_PATH=YandexInternalRootCA.crt

# URL вашего сервера
SERVER_URL=https://server.asktab.ru
```

Сохраните файл: `Ctrl+O`, `Enter`, `Ctrl+X`.

---

## Шаг 6. Проверка подключения к ClickHouse

```bash
source venv/bin/activate
python3 -c "
from config import *
import clickhouse_connect

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE, secure=True, verify=False
)
print('Tables:', client.query('SHOW TABLES').result_rows[:5])
"
```

---

## Шаг 7. Настройка systemd (автозапуск сервиса)

```bash
# Копируем unit-файл
cp /root/clickhouse_analytics_agent/agent.service /etc/systemd/system/analytics-agent.service

# Перезагружаем конфигурацию systemd
systemctl daemon-reload

# Включаем автозапуск при загрузке сервера
systemctl enable analytics-agent

# Запускаем сервис
systemctl start analytics-agent

# Проверяем статус
systemctl status analytics-agent
```

Просмотр логов в реальном времени:
```bash
journalctl -u analytics-agent -f
```

---

## Шаг 8. Настройка Nginx

```bash
# Копируем конфиг
cp /root/clickhouse_analytics_agent/nginx.conf /etc/nginx/sites-available/analytics-agent

# Включаем сайт
ln -sf /etc/nginx/sites-available/analytics-agent /etc/nginx/sites-enabled/analytics-agent

# Удаляем дефолтный сайт (если мешает)
rm -f /etc/nginx/sites-enabled/default

# Проверяем конфиг
nginx -t

# Перезагружаем Nginx
systemctl reload nginx
```

---

## Шаг 9. Получение HTTPS сертификата (Let's Encrypt)

> ⚠️ DNS-запись `server.asktab.ru → IP сервера` должна уже работать!

```bash
certbot --nginx -d server.asktab.ru
```

Certbot сам обновит nginx.conf, добавив пути к сертификатам.

Проверка автообновления:
```bash
certbot renew --dry-run
```

---

## Шаг 10. Проверка работы API

```bash
# Health check
curl https://server.asktab.ru/health

# Информация об API
curl https://server.asktab.ru/api/info

# Тестовый запрос к агенту
curl -X POST https://server.asktab.ru/api/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Привет! Какие таблицы есть в базе данных?",
    "session_id": "test-session-001"
  }'
```

Документация API (Swagger UI):
```
https://server.asktab.ru/docs
```

---

## Управление сервисом

| Команда | Описание |
|---------|----------|
| `systemctl start analytics-agent` | Запустить |
| `systemctl stop analytics-agent` | Остановить |
| `systemctl restart analytics-agent` | Перезапустить |
| `systemctl status analytics-agent` | Статус |
| `journalctl -u analytics-agent -f` | Логи в реальном времени |
| `journalctl -u analytics-agent --since "1 hour ago"` | Логи за последний час |

---

## Обновление агента

```bash
cd /root/repo
git pull origin main

# Обновляем файлы агента
cp -r clickhouse_analytics_agent/* /root/clickhouse_analytics_agent/

# Обновляем зависимости если нужно
source /root/clickhouse_analytics_agent/venv/bin/activate
pip install -r /root/clickhouse_analytics_agent/requirements.txt

# Перезапускаем сервис
systemctl restart analytics-agent
systemctl status analytics-agent
```

---

## Структура файлов

```
/root/clickhouse_analytics_agent/
├── .env                    ← ваши секреты (не в git!)
├── .env.example            ← шаблон
├── config.py               ← загрузка конфигурации
├── clickhouse_client.py    ← подключение к ClickHouse + выгрузка Parquet
├── python_sandbox.py       ← выполнение Python кода, захват графиков
├── tools.py                ← LangGraph инструменты (3 tools)
├── agent.py                ← LangGraph агент + SqliteSaver
├── api_server.py           ← FastAPI сервер
├── requirements.txt        ← зависимости Python
├── agent.service           ← systemd unit
├── nginx.conf              ← конфиг Nginx
├── YandexInternalRootCA.crt ← SSL сертификат Яндекс Cloud
├── chat_history.db         ← SQLite (создаётся автоматически)
└── temp_data/              ← временные parquet файлы (автоочистка)
```

---

## Диагностика проблем

### Сервис не запускается
```bash
journalctl -u analytics-agent -n 50 --no-pager
# Обычные причины: неверный .env, ошибка подключения к ClickHouse
```

### Ошибка подключения к ClickHouse
```bash
# Проверьте доступность хоста
nc -zv your-cluster.mdb.yandexcloud.net 8443
# Проверьте сертификат
openssl s_client -connect your-cluster.mdb.yandexcloud.net:8443 -CAfile YandexInternalRootCA.crt
```

### 502 Bad Gateway в Nginx
```bash
# Проверьте, запущен ли uvicorn
systemctl status analytics-agent
# Проверьте, слушает ли порт 8000
ss -tlnp | grep 8000
```

### Медленные ответы
- Нормальное время ответа: 15–60 секунд (агент делает несколько вызовов LLM + ClickHouse)
- Увеличьте `proxy_read_timeout` в nginx.conf если получаете 504

---

## Firewall (UFW)

```bash
ufw allow 22/tcp    # SSH
ufw allow 80/tcp    # HTTP (редирект на HTTPS)
ufw allow 443/tcp   # HTTPS
ufw enable
ufw status
```

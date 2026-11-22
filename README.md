# Індивідуальний проєкт з обробки великих даних: нестабільність тривалості поїздок за зонами прибуття

## 1. Дані та відтворюваність

- **Датасет:** NYC TLC Trip Record Data — Yellow Taxi Trip Records (12 місяців, об'єднаний нестиснений CSV, `4.8 ГБ`, `~49 млн` рядків).
- **Джерело:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Ліцензія:** Open Data / Public Domain (NYC Open Data Terms of Use / CC0).
- **Контрольна сума повного CSV (`sha256sum`):** вміст файлу `yellow_tripdata_12m.csv.sha256`
- **Вибірка:** `data/yellow_tripdata_sample.csv` (згенерована скриптом `bin/make_sample.py`).

### Примітка

Повний датасет було отримано шляхом об'єднання файлів за кожен місяць 2025 року насутпною командою:

```bash
duckdb -c "
COPY (
  SELECT * FROM read_parquet('yellow_tripdata_*.parquet', union_by_name=true)
) TO 'yellow_tripdata_12m.csv' (HEADER true, DELIMITER ',');
"
```

## 2. Структура репозиторію

- `docker-compose.yml` — інфраструктура Spark та History Server із зафіксованими версіями образів.
- `conf/spark-defaults.conf` — конфігурація Spark
- `bin/process_file.sh` — скрипт запуску аналізу
- `bin/make_sample.py` — генерація детермінованої вибірки з повного датасету.
- `pipeline.py` — реалізація багатоетапного пайплайну на PySpark.
- `artifacts/full_run/` — журнали подій Spark (event logs) і результати повного запуску.

## 3. Команди запуску

### Підняття контейнерів

```bash
docker compose up -d
```

### Генерація вибірки
```bash
python3 bin/make_sample.py \
  --input data/yellow_tripdata_12m.csv \
  --output data/yellow_tripdata_sample.csv \
  --rate 0.05 \
  --seed 42
```

### Запуск на вибірці (очікуваний час: 1-2 хвилини)

```bash
./bin/process_file.sh /data/yellow_tripdata_sample.csv
```

### Запуск на повному датасеті

```bash
./bin/process_file.sh /data/yellow_tripdata_12m.csv
```

## 4. Очікуваний результат

Пайплайн формує агреговані метрики по зонах прибуття (DOLocationID): загальну кількість валідних поїздок, кількість/частку поїздок із перевищенням норми маршруту (PULocationID, DOLocationID, hour_of_day) та середній коефіцієнт затримки (ratio).

Результати записуються до директорії виводу, а повні event logs зберігаються в spark-events/.

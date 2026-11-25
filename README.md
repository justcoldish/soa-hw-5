# Online Cinema: Kafka + ClickHouse + Grafana

## Запуск

```
docker compose up -d --build
```

Поднимаются три брокера Kafka, Schema Registry, ClickHouse, Postgres с миграциями, S3 на RustFS, продюсер с генератором событий, агрегатор и Grafana. У всех health checks, сервисы стартуют после зависимостей

- Grafana: http://localhost:3000, дашборд открывается без логина, для правки admin / admin
- Swagger продюсера: http://localhost:8010/docs
- Swagger агрегатора: http://localhost:8020/docs
- Консоль S3: http://localhost:9001, логин rustfsadmin / rustfsadmin

Порты меняются через PRODUCER_PORT, AGGREGATOR_PORT, GRAFANA_PORT, SCHEMA_REGISTRY_PORT, S3_CONSOLE_PORT

```
GRAFANA_PORT=3001 docker compose up -d --build
```

Стоп

```
docker compose down -v
```

## Тесты

Событие через продюсер доходит до ClickHouse с теми же полями, кривое событие получает 422. Тест пишет событие с уникальными id за 2020 год, поэтому его можно гонять много раз

```
docker compose run --rm test
```

## Решения

- Схема события в Avro, progress_seconds обязателен, у поиска он 0. Продюсер регистрирует ее в Schema Registry при старте и пишет в лог версию. ClickHouse берет схему из реестра по id в каждом сообщении
- Ключ партиционирования user_id: события одного пользователя идут по порядку в одной партиции, а DAU и retention считаются по пользователям. movie_id дал бы перекос в партицию с популярным фильмом
- Три брокера, replication factor 3, min.insync.replicas 2. Кворуму контроллеров KRaft из двух узлов нужны оба, и падение одного останавливает кластер. С тремя брокерами запись продолжается без одного, а подтвержденное сообщение лежит минимум на двух
- Продюсер ждет подтверждения от всех реплик в ISR, включена идемпотентность. Ретраи идут 30 секунд, пауза растет вдвое от 100ms до 5s. HTTP отвечает id события только после подтверждения, иначе 503
- Генератор раскидывает сессии по последней неделе, чтобы когортная таблица заполнилась сразу. Внутри сессии поиск, старт, паузы, финиш и лайк идут по порядку, прогресс растет
- В ClickHouse Kafka Engine читает топик, materialized view перекладывает события в MergeTree. Партиции по месяцу, сортировка по дню, типу события и пользователю, потому что агрегаты считаются по дням
- Агрегаты считаются в ClickHouse агрегатными и оконными функциями и пишутся в таблицы ReplacingMergeTree. Версия строки это время расчета, поэтому пересчет заменяет строки, а не дублирует
- Когорта это день первого события пользователя. Retention за дни с 0 по 7 пишется метриками retention_d0 ... retention_d7 на дату когорты, когортная таблица в Grafana строится из них
- Агрегатор раз в минуту пересчитывает последние 8 дней, потому что retention D7 когорты готов только через неделю. Интервал задает AGGREGATION_INTERVAL_SECONDS. Результат идет в Postgres через upsert, ошибку записи агрегатор повторяет 3 раза
- Каждый цикл агрегатор перезаписывает в S3 файл за вчера, поэтому упавший экспорт повторится в следующем цикле. В JSON лежат все метрики дня и топ фильмов со временем расчета
- MinIO больше не публикует образы, вместо него S3-совместимый RustFS
- Grafana читает только таблицы агрегатов

## Демо

```
API=localhost:8010
AGG=localhost:8020
```

Событие, событие без id и времени, кривой тип, кривое тело

```
curl $API/events --json '{"user_id":"u1","movie_id":"movie-1","event_type":"VIEW_STARTED","device_type":"TV","session_id":"s1","progress_seconds":0,"timestamp":"2026-11-25T10:00:00Z"}'
curl $API/events --json '{"user_id":"u1","movie_id":"movie-1","event_type":"LIKED","device_type":"TV","session_id":"s1"}'
curl $API/events --json '{"user_id":"u1","movie_id":"movie-1","event_type":"DANCED","device_type":"TV","session_id":"s1"}'
curl $API/events --json '{"user_id":"u1"}'
```

Схема в реестре и топик

```
curl localhost:8081/subjects/movie-events-value/versions/1
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic movie-events
```

Ручной пересчет и экспорт за день

```
curl -X POST "$AGG/recompute?day=$(date -u +%F)"
curl -X POST "$AGG/export?day=$(date -u +%F)"
```

Файлы в S3

```
s3() { docker run --rm --network soa-hw-5_default -e AWS_ACCESS_KEY_ID=rustfsadmin -e AWS_SECRET_ACCESS_KEY=rustfsadmin -e AWS_REGION=us-east-1 amazon/aws-cli --endpoint-url http://s3:9000 s3 "$@"; }
s3 ls --recursive s3://movie-analytics
s3 cp s3://movie-analytics/daily/$(date -u +%F)/aggregates.json -
```

Падение брокера: запись продолжается, в ISR остаются два брокера

```
docker compose stop kafka-2
curl $API/events --json '{"user_id":"u2","movie_id":"movie-2","event_type":"LIKED","device_type":"MOBILE","session_id":"s2"}'
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic movie-events
docker compose start kafka-2
```

## БД

```
docker compose exec -T clickhouse clickhouse-client -q 'SELECT event_type, count() FROM movie_events GROUP BY event_type'
docker compose exec -T clickhouse clickhouse-client -q 'SELECT * FROM daily_metrics FINAL ORDER BY date, metric FORMAT PrettyCompact'
docker compose exec -T postgres psql -U postgres analytics -c 'SELECT * FROM daily_metrics ORDER BY date, metric' -c 'SELECT * FROM top_movies ORDER BY date, rank'
```

Зайти руками:

```
docker compose exec clickhouse clickhouse-client
```

```
docker compose exec postgres psql -U postgres analytics
```

## Логи

Публикации в продюсере, циклы пересчета и экспорт в агрегаторе

```
docker compose logs -f producer aggregator
```

import json
import logging
import os
import threading
import time
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta

import boto3
import clickhouse_connect
import psycopg
import uvicorn
from botocore.config import Config
from fastapi import FastAPI
from psycopg.rows import dict_row

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger()
BUCKET = "movie-analytics"

# без сессии ClickHouse не блокирует параллельные запросы ручного пересчета и расписания
ch = clickhouse_connect.get_client(host="clickhouse", autogenerate_session_id=False)
s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"], aws_access_key_id=os.environ["S3_ACCESS_KEY"],
                  aws_secret_access_key=os.environ["S3_SECRET_KEY"], region_name="us-east-1",
                  config=Config(s3={"addressing_style": "path"}))
with suppress(s3.exceptions.BucketAlreadyOwnedByYou, s3.exceptions.BucketAlreadyExists):
    s3.create_bucket(Bucket=BUCKET)
app = FastAPI(title="Aggregation Service")

AGGREGATES = [
    """INSERT INTO daily_metrics SELECT {day:Date}, m.1, ifNotFinite(m.2, 0), now() FROM (
        SELECT uniq(user_id) AS dau,
            avgIf(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_watch,
            countIf(event_type = 'VIEW_FINISHED') / countIf(event_type = 'VIEW_STARTED') AS conversion
        FROM movie_events WHERE toDate(timestamp) = {day:Date}
    ) ARRAY JOIN [('dau', toFloat64(dau)), ('avg_watch_seconds', avg_watch), ('view_conversion', conversion)] AS m""",
    """INSERT INTO top_movies
    SELECT {day:Date}, row_number() OVER (ORDER BY views DESC, movie_id), movie_id, views, now() FROM (
        SELECT movie_id, uniqExact(session_id) AS views FROM movie_events
        WHERE toDate(timestamp) = {day:Date} AND event_type = 'VIEW_STARTED'
        GROUP BY movie_id ORDER BY views DESC, movie_id LIMIT 10)""",
    # max(users) это размер когорты: в day 0 активны все. WITH FILL ставит 0 в прошедшие дни без возвратов
    """INSERT INTO daily_metrics
    SELECT {day:Date}, concat('retention_d', toString(day)), users / max(users) OVER (), now() FROM (
        SELECT dateDiff('day', cohort_date, toDate(timestamp)) AS day, uniqExact(user_id) AS users
        FROM (SELECT user_id, timestamp, min(toDate(timestamp)) OVER (PARTITION BY user_id) AS cohort_date FROM movie_events)
        WHERE cohort_date = {day:Date} AND day <= 7
        GROUP BY day ORDER BY day WITH FILL TO least(8, dateDiff('day', {day:Date}, today()) + 1))""",
]


@app.post("/recompute")
def recompute(day: date):
    log.info("recompute %s: start", day)
    started = time.monotonic()
    events = ch.command("SELECT count() FROM movie_events WHERE toDate(timestamp) = {day:Date}", {"day": day})
    for sql in AGGREGATES:
        ch.command(sql, {"day": day})
    metrics = ch.query("SELECT date, metric, value, computed_at FROM daily_metrics FINAL WHERE date = {day:Date}", {"day": day})
    top = ch.query("SELECT date, rank, movie_id, views, computed_at FROM top_movies FINAL WHERE date = {day:Date}", {"day": day})
    for attempt in range(1, 4):
        try:
            with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
                conn.cursor().executemany("""INSERT INTO daily_metrics VALUES (%s, %s, %s, %s) ON CONFLICT (date, metric)
                    DO UPDATE SET value = excluded.value, computed_at = excluded.computed_at""", metrics.result_rows)
                conn.cursor().executemany("""INSERT INTO top_movies VALUES (%s, %s, %s, %s, %s) ON CONFLICT (date, rank)
                    DO UPDATE SET movie_id = excluded.movie_id, views = excluded.views, computed_at = excluded.computed_at""",
                                          top.result_rows)
            break
        except psycopg.OperationalError as e:
            log.warning("recompute %s: postgres attempt %d failed: %s", day, attempt, e)
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)
    seconds = round(time.monotonic() - started, 2)
    log.info("recompute %s: done, %d events, %d metrics, %d top movies in %ss", day, events, len(metrics.result_rows),
             len(top.result_rows), seconds)
    return {"date": day, "events": events, "seconds": seconds}


@app.post("/export")
def export(day: date):
    with psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row) as conn:
        body = {"date": day,
                "metrics": conn.execute("SELECT metric, value, computed_at FROM daily_metrics WHERE date = %s ORDER BY metric", [day]).fetchall(),
                "top_movies": conn.execute("SELECT rank, movie_id, views, computed_at FROM top_movies WHERE date = %s ORDER BY rank", [day]).fetchall()}
    key = f"daily/{day}/aggregates.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(body, default=str, indent=2), ContentType="application/json")
    log.info("export %s: s3://%s/%s, %d metrics", day, BUCKET, key, len(body["metrics"]))
    return {"key": f"s3://{BUCKET}/{key}"}


def schedule():
    while True:
        today = datetime.now(UTC).date()
        # retention D7 когорты готов только через 7 дней
        for n in range(8):
            try:
                recompute(today - timedelta(days=n))
            except Exception:
                log.exception("recompute failed, retry next cycle")
        try:
            export(today - timedelta(days=1))
        except Exception:
            log.exception("export failed, retry next cycle")
        time.sleep(float(os.environ["AGGREGATION_INTERVAL_SECONDS"]))


threading.Thread(target=schedule, daemon=True).start()
uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False)

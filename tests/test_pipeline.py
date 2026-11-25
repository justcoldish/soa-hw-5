import json
import time
import urllib.error
import urllib.request
from uuid import uuid4


def post(url, body):
    request = urllib.request.Request(url, json.dumps(body).encode(), {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


# уникальные id изолируют прогоны друг от друга, чистить таблицы не нужно
event = {"user_id": f"test-{uuid4()}", "movie_id": "movie-test", "event_type": "VIEW_FINISHED",
         "timestamp": "2020-01-01T10:00:00.123Z", "device_type": "TV", "session_id": str(uuid4()), "progress_seconds": 4321}
status, body = post("http://producer:8000/events", event)
assert status == 200, body
sql = f"SELECT * FROM movie_events WHERE event_id = '{body['event_id']}' FORMAT JSONEachRow"
for _ in range(60):
    if rows := urllib.request.urlopen("http://clickhouse:8123", sql.encode()).read().splitlines():
        break
    time.sleep(0.5)
assert [json.loads(r) for r in rows] == [event | {"event_id": body["event_id"], "timestamp": "2020-01-01 10:00:00.123"}], rows
print("ok: event reached ClickHouse with the same fields")

status, _ = post("http://producer:8000/events", {"user_id": "u", "movie_id": "m", "event_type": "DANCED",
                                                 "device_type": "TV", "session_id": "s"})
assert status == 422, status
print("ok: invalid event rejected with 422")

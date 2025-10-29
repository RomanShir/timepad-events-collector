import csv
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests

API_URL = "https://api.timepad.ru/v1/events"
TOKEN = "850c61936dc141589e00c51f5d0da37dfb4b1aaa"

DEFAULT_FIELDS = ",".join([
    "id","title","starts_at","ends_at","categories",
    "organization","location","price_min","price_max","url","created_at"
])
ISO_8601_ZULU = "%Y-%m-%dT%H:%M:%SZ"
EXPORT_COLUMNS = [
    "starts_at",
    "name",
    "description_short",
    "organization",
    "categories",
    "tickets_limit",
    "moderation_status",
]

def read_filters(path="filters.csv"):
    filters = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            k = (row.get("key") or "").strip()
            v = (row.get("value") or "").strip()
            if not k:
                continue
            if v.startswith("'"):  # убрать апостроф
                v = v[1:]
            filters[k] = v
    for time_key in ("starts_at_min", "starts_at_max"):
        value = filters.get(time_key)
        if value:
            try:
                datetime.strptime(value, ISO_8601_ZULU)
            except ValueError as exc:
                raise ValueError(
                    f"Значение {time_key} должно быть в формате ISO 8601 YYYY-MM-DDTHH:MM:SSZ, получено: {value}"
                ) from exc
    return filters

def build_query(filters: dict, *, fields: str | None = None):
    params = dict(filters)
    if fields is not None:
        params["fields"] = fields
    elif "fields" not in params:
        params["fields"] = DEFAULT_FIELDS
    return urlencode(params, doseq=True, safe=":")

def request_json_with_retry(url: str, headers: dict, max_attempts: int = 3):
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as exc:
            if attempt == max_attempts:
                print("Ошибка соединения:", exc)
                raise
            wait = 2 ** (attempt - 1)
            print(f"⚠️  Ошибка соединения ({exc}), повтор через {wait} с...")
            time.sleep(wait)
            continue

        if response.status_code == 200:
            try:
                return response.json()
            except ValueError as exc:
                print("Ошибка чтения JSON:", exc)
                raise

        if response.status_code >= 500 or response.status_code == 429:
            if attempt == max_attempts:
                print("Ошибка запроса:", response.status_code, response.text[:300])
                response.raise_for_status()
            wait = 2 ** (attempt - 1)
            print(f"⚠️  Временная ошибка {response.status_code}, повтор через {wait} с...")
            time.sleep(wait)
            continue

        print("Ошибка запроса:", response.status_code, response.text[:300])
        response.raise_for_status()

    raise RuntimeError("Не удалось получить ответ от API Timepad")

class RateLimiter:
    def __init__(self, min_delay: float = 0.5, max_delay: float = 1.0, multiplier_cap: float = 10.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.multiplier_cap = multiplier_cap
        self._lock = threading.Lock()
        self._next_time = 0.0
        self._multiplier = 1.0

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            wait_until = max(self._next_time, now)
            sleep_for = max(0.0, wait_until - now)
            delay = random.uniform(self.min_delay, self.max_delay) * self._multiplier
            self._next_time = wait_until + delay
        if sleep_for > 0:
            time.sleep(sleep_for)

    def increase_delay(self, factor: float = 1.5):
        with self._lock:
            self._multiplier = min(self._multiplier * factor, self.multiplier_cap)

    def schedule_pause(self, pause_seconds: float):
        with self._lock:
            now = time.monotonic()
            self._next_time = max(self._next_time, now) + pause_seconds

def fetch_event_ids(filters):
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    base_filters = dict(filters)
    base_filters.pop("fields", None)
    skip_in_initial = "skip" in base_filters
    skip = int(base_filters.pop("skip", 0) or 0)
    limit = int(base_filters.get("limit", 100))

    event_ids = []
    page = 1

    while True:
        page_filters = dict(base_filters)
        if skip_in_initial or skip:
            page_filters["skip"] = skip
        query = build_query(page_filters, fields="id")
        url = f"{API_URL}?{query}"
        print("Запрос списка событий:", url)
        data = request_json_with_retry(url, headers=headers)

        values = data.get("values", [])
        ids_on_page = [item.get("id") for item in values if item.get("id") is not None]
        event_ids.extend(ids_on_page)
        print(f"Страница {page}: найдено id {len(ids_on_page)}, всего собрано {len(event_ids)}")

        if len(values) < limit:
            break

        skip += limit
        page += 1

    return event_ids

def fetch_single_event(event_id, headers, rate_limiter: RateLimiter, max_attempts: int = 5):
    url = f"{API_URL}/{event_id}"
    for attempt in range(1, max_attempts + 1):
        rate_limiter.acquire()
        try:
            response = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as exc:
            if attempt == max_attempts:
                print(f"❌  Ошибка соединения для события {event_id}: {exc}")
                return None
            wait = min(5, 2 ** attempt)
            print(f"⚠️  Ошибка соединения для события {event_id} ({exc}), повтор через {wait} с...")
            time.sleep(wait)
            continue

        if response.status_code == 200:
            try:
                data = response.json()
            except ValueError as exc:
                print(f"⚠️  Ошибка JSON для события {event_id}: {exc}")
                return None
            payload = data.get("value") if isinstance(data, dict) else None
            if payload is None:
                payload = data
            return extract_event_row(payload)

        snippet = response.text[:300] if response.text else ""
        is_rate_limited = (
            response.status_code in RATE_LIMIT_STATUS or
            "Too Many Requests" in snippet
        )
        if is_rate_limited:
            rate_limiter.increase_delay()
            pause_header = response.headers.get("Retry-After")
            pause = None
            if pause_header:
                try:
                    pause = float(pause_header)
                except ValueError:
                    pause = None
            if pause is None:
                pause = random.uniform(30, 60)
            else:
                pause = max(30.0, min(pause, 60.0))
            print(f"⏳  Ограничение скорости для события {event_id} (код {response.status_code}). Пауза {pause:.1f} с")
            rate_limiter.schedule_pause(pause)
            time.sleep(pause)
            continue

        if response.status_code == 404:
            print(f"ℹ️  Событие {event_id} не найдено (404).")
            return None

        print(f"⚠️  Ошибка ответа {response.status_code} для события {event_id}: {snippet}")
        if attempt == max_attempts:
            return None
        wait = min(5, 2 ** attempt)
        time.sleep(wait)

    return None

RATE_LIMIT_STATUS = {429, 502, 503}

def fetch_event_details(event_ids):
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    rate_limiter = RateLimiter()
    collected = {}

    def process_batch(batch_ids, label: str):
        batch_results = {}
        batch_failed = []
        if not batch_ids:
            return batch_results, batch_failed

        print(f"{label}: обработка {len(batch_ids)} событий")
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_id = {
                executor.submit(fetch_single_event, event_id, headers, rate_limiter): event_id
                for event_id in batch_ids
            }
            for future in as_completed(future_to_id):
                event_id = future_to_id[future]
                try:
                    result_row = future.result()
                except Exception as exc:
                    print(f"⚠️  Сбой при обработке события {event_id}: {exc}")
                    batch_failed.append(event_id)
                    continue
                if result_row is None:
                    batch_failed.append(event_id)
                else:
                    batch_results[event_id] = result_row
        return batch_results, batch_failed

    def write_failed_ids(ids):
        if ids:
            try:
                with open("failed_ids.txt", "w", encoding="utf-8") as fh:
                    for event_id in ids:
                        fh.write(f"{event_id}\n")
            except OSError as exc:
                print(f"⚠️  Не удалось записать failed_ids.txt: {exc}")
        else:
            if os.path.exists("failed_ids.txt"):
                try:
                    os.remove("failed_ids.txt")
                except OSError:
                    pass

    # Основная загрузка
    primary_results, primary_failed = process_batch(event_ids, "Основная загрузка")
    collected.update(primary_results)
    write_failed_ids(primary_failed)

    retries_left = 2
    remaining_ids = primary_failed
    attempt = 1

    while remaining_ids and retries_left > 0:
        rate_limiter.increase_delay()
        pause = random.uniform(30, 60)
        print(f"⏸  Пауза перед повторной попыткой: {pause:.1f} с")
        rate_limiter.schedule_pause(pause)
        time.sleep(pause)

        label = f"Повторная загрузка #{attempt}"
        retry_results, retry_failed = process_batch(remaining_ids, label)
        collected.update(retry_results)
        remaining_ids = retry_failed
        write_failed_ids(remaining_ids)

        retries_left -= 1
        attempt += 1

    if remaining_ids:
        print(f"⚠️  Не удалось загрузить данные для {len(remaining_ids)} событий. См. failed_ids.txt")
    else:
        print("✅ Все события успешно обработаны.")

    ordered_rows = [collected[event_id] for event_id in event_ids if event_id in collected]
    return ordered_rows

def extract_event_row(event):
    categories = event.get("categories") or []
    category_names = []
    for item in categories:
        if isinstance(item, dict):
            name = item.get("name")
            if name:
                category_names.append(str(name))
            elif item.get("id") is not None:
                category_names.append(str(item.get("id")))
        elif item:
            category_names.append(str(item))
    categories_value = ", ".join(category_names)

    organization_name = ""
    organization = event.get("organization")
    if isinstance(organization, dict):
        organization_name = organization.get("name") or ""

    tickets_limit = event.get("tickets_limit")
    if tickets_limit is None:
        tickets_limit_value = ""
    else:
        tickets_limit_value = str(tickets_limit)

    return {
        "starts_at": event.get("starts_at") or "",
        "name": event.get("name") or "",
        "description_short": event.get("description_short") or "",
        "organization": organization_name,
        "categories": categories_value,
        "tickets_limit": tickets_limit_value,
        "moderation_status": event.get("moderation_status") or "",
    }

def main():
    filters = read_filters("filters.csv")
    event_ids = fetch_event_ids(filters)
    if not event_ids:
        print("⚠️  Не найдено событий с указанными фильтрами.")
        return

    print(f"Всего найдено id событий: {len(event_ids)}")
    events = fetch_event_details(event_ids)

    df = pd.DataFrame(events, columns=EXPORT_COLUMNS)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    city_name = filters.get("cities", "all").replace(" ", "_")
    out = f"timepad_events_{city_name}_{stamp}.csv"
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"✅ Сохранено {len(df)} событий в файл {out}")

if __name__ == "__main__":
    main()

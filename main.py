import csv
import os
import random
import re
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Set
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
REGISTRY_FILE = Path("events_registry.csv")
DEFAULT_DATA_FILE = Path("events_data.csv")
DATA_FILE_MAP = {
    "санктпетербург": Path("events_data_spb.csv"),
    "spb": Path("events_data_spb.csv"),
    "москва": Path("events_data_msk.csv"),
    "moscow": Path("events_data_msk.csv"),
    "moskva": Path("events_data_msk.csv"),
    "msk": Path("events_data_msk.csv"),
}
REGISTRY_COLUMNS = ["id", "starts_at"]
EXPORT_COLUMNS = [
    "id",
    "starts_at",
    "name",
    "url",
    "description_short",
    "description_html",
    "organization",
    "categories",
    "tickets_limit",
    "moderation_status",
]

def normalize_city_key(city: str | None) -> str:
    if not city:
        return ""
    key = city.strip().lower()
    key = key.replace("ё", "е")
    key = key.replace(" ", "")
    key = key.replace("-", "")
    return key

def resolve_data_file(city: str | None) -> Path:
    key = normalize_city_key(city)
    return DATA_FILE_MAP.get(key, DEFAULT_DATA_FILE)

def format_human_datetime(raw_value) -> str:
    if raw_value is None:
        return ""
    if isinstance(raw_value, float) and pd.isna(raw_value):
        return ""
    text = str(raw_value).strip()
    if not text:
        return ""
    candidate = text
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+0000"
    if re.search(r"[+-]\d{2}:\d{2}$", candidate):
        candidate = candidate[:-3] + candidate[-2:]
    try:
        dt = datetime.strptime(candidate, "%Y-%m-%dT%H:%M:%S%z")
        return dt.strftime("%d.%m.%Y %H:%M")
    except ValueError:
        pass
    try:
        dt = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S")
        return dt.strftime("%d.%m.%Y %H:%M")
    except ValueError:
        pass
    try:
        dt = datetime.strptime(text, "%d.%m.%Y %H:%M")
        return dt.strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return text

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

    events_summary = []
    page = 1

    while True:
        page_filters = dict(base_filters)
        if skip_in_initial or skip:
            page_filters["skip"] = skip
        query = build_query(page_filters, fields="id,starts_at")
        url = f"{API_URL}?{query}"
        print("Запрос списка событий:", url)
        data = request_json_with_retry(url, headers=headers)

        values = data.get("values", [])
        page_records = []
        for item in values:
            event_id = item.get("id")
            if event_id is None:
                continue
            try:
                event_id_int = int(event_id)
            except (TypeError, ValueError):
                continue
            page_records.append({
                "id": event_id_int,
                "starts_at": item.get("starts_at"),
            })
        events_summary.extend(page_records)
        print(f"Страница {page}: найдено id {len(page_records)}, всего собрано {len(events_summary)}")

        if len(values) < limit:
            break

        skip += limit
        page += 1

    return events_summary

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

        print(f"ℹ️  Ответ {response.status_code} для события {event_id}")
        if response.status_code == 200:
            try:
                data = response.json()
            except ValueError as exc:
                print(f"⚠️  Ошибка JSON для события {event_id}: {exc}")
                return None
            payload = data.get("value") if isinstance(data, dict) else None
            if payload is None:
                payload = data
            print(f"✅  Успешный ответ для события {event_id} (200)")
            return extract_event_row(event_id, payload)

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
                    traceback.print_exc()
                    batch_failed.append(event_id)
                    continue
                if result_row is None:
                    print(f"⚠️  Пустой результат для события {event_id}, добавляю в повторную очередь")
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

def ensure_registry(events: Iterable[dict]) -> List[int]:
    """Append new events to the registry and return the full unique id set."""
    prepared_records = []
    seen_new: Set[int] = set()
    for event in events:
        event_id = event.get("id")
        if event_id is None:
            continue
        try:
            event_id_int = int(event_id)
        except (TypeError, ValueError):
            continue
        if event_id_int in seen_new:
            continue
        starts_at_formatted = format_human_datetime(event.get("starts_at"))
        prepared_records.append({
            "id": event_id_int,
            "starts_at": starts_at_formatted,
        })
        seen_new.add(event_id_int)

    existing_records = []
    existing_index = {}
    if REGISTRY_FILE.exists():
        df_existing = pd.read_csv(REGISTRY_FILE, dtype={"id": "int64"}, keep_default_na=False)
        if "starts_at" not in df_existing.columns:
            df_existing["starts_at"] = ""
        for idx, row in df_existing.iterrows():
            event_id = int(row["id"])
            formatted = format_human_datetime(row.get("starts_at", ""))
            existing_records.append({"id": event_id, "starts_at": formatted})
            existing_index[event_id] = idx

    for record in prepared_records:
        event_id = record["id"]
        if event_id in existing_index:
            idx = existing_index[event_id]
            existing_row = existing_records[idx]
            if not existing_row.get("starts_at") and record["starts_at"]:
                existing_row["starts_at"] = record["starts_at"]
        else:
            existing_index[event_id] = len(existing_records)
            existing_records.append(record)

    if not existing_records:
        existing_records = prepared_records

    df = pd.DataFrame(existing_records, columns=REGISTRY_COLUMNS)
    df.to_csv(REGISTRY_FILE, index=False, encoding="utf-8")
    print(f"✅ Обновлен реестр событий: {len(existing_records)} уникальных id")
    return [record["id"] for record in existing_records]

def load_registry_ids() -> List[int]:
    if not REGISTRY_FILE.exists():
        return []
    df = pd.read_csv(REGISTRY_FILE, dtype={"id": "int64"}, keep_default_na=False)
    if "starts_at" in df.columns:
        df["starts_at"] = df["starts_at"].apply(format_human_datetime)
    ids = df["id"].dropna().astype("int64").tolist()
    return ids

def load_existing_data_ids(data_file: Path) -> Set[int]:
    if not data_file.exists():
        return set()
    df = pd.read_csv(data_file, keep_default_na=False)
    if "id" not in df.columns:
        return set()
    ids_series = pd.to_numeric(df["id"], errors="coerce").dropna().astype("int64")
    ids = set(ids_series.tolist())
    return ids

def append_events_data(rows: List[dict], data_file: Path):
    if not rows:
        return
    df_new = pd.DataFrame(rows)
    missing_in_new = [col for col in EXPORT_COLUMNS if col not in df_new.columns]
    for col in missing_in_new:
        df_new[col] = ""
    df_new["starts_at"] = df_new["starts_at"].apply(format_human_datetime)
    df_new = df_new[EXPORT_COLUMNS]

    if data_file.exists():
        df_existing = pd.read_csv(data_file, keep_default_na=False)
        for col in EXPORT_COLUMNS:
            if col not in df_existing.columns:
                df_existing[col] = ""
        df_existing["starts_at"] = df_existing["starts_at"].apply(format_human_datetime)
        df_existing = df_existing[EXPORT_COLUMNS]
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new
    df_combined = df_combined[EXPORT_COLUMNS]
    df_combined.to_csv(data_file, index=False, encoding="utf-8", columns=EXPORT_COLUMNS)
    print(f"✅ Дописано записей в {data_file}: {len(df_new)}")

def extract_event_row(event_id, event):
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
        "id": event_id,
        "starts_at": format_human_datetime(event.get("starts_at")),
        "name": event.get("name") or "",
        "url": event.get("url") or "",
        "description_short": event.get("description_short") or "",
        "description_html": event.get("description_html") or "",
        "organization": organization_name,
        "categories": categories_value,
        "tickets_limit": tickets_limit_value,
        "moderation_status": event.get("moderation_status") or "",
    }

def main():
    filters = read_filters("filters.csv")
    data_file = resolve_data_file(filters.get("cities"))

    events_summary = fetch_event_ids(filters)
    if not events_summary:
        print("⚠️  Не найдено событий с указанными фильтрами.")
        return

    registry_ids = ensure_registry(events_summary)

    existing_data_ids = load_existing_data_ids(data_file)
    pending_ids = [event_id for event_id in registry_ids if event_id not in existing_data_ids]

    if not pending_ids:
        print(f"ℹ️  Новых событий для загрузки нет. {data_file.name} уже содержит все записи.")
        return

    print(f"Всего событий в реестре: {len(registry_ids)}, предстоит загрузить: {len(pending_ids)}")
    rows = fetch_event_details(pending_ids)
    append_events_data(rows, data_file)

if __name__ == "__main__":
    main()

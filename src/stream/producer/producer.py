import json
import os
import sqlite3
import time
from collections import defaultdict

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka1:19092")
DB_PATH = os.environ.get("DB_PATH", "/data/measurements.db")
HOUR_INTERVAL = float(os.environ.get("STREAM_INTERVAL_SECONDS", "1"))

AQ_TOPIC = "air_quality_stream"
WX_TOPIC = "weather_stream"


def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}", flush=True)
            return producer
        except NoBrokersAvailable:
            print("Kafka not available yet, retrying in 3s...", flush=True)
            time.sleep(3)


def load_aq_rows(conn):
    """Return air quality rows grouped by hour bucket (YYYY-MM-DD HH)."""
    cur = conn.execute(
        """
        SELECT
            strftime('%Y-%m-%d %H', collected_at) AS hour_bucket,
            collected_at,
            city,
            state,
            aqi,
            aqi_category,
            dominant_pollutant,
            pm25,
            pm10,
            o3,
            no2,
            so2,
            co
        FROM air_quality
        ORDER BY hour_bucket, collected_at
        """
    )
    rows_by_hour = defaultdict(list)
    for row in cur.fetchall():
        hour, ts, city, state, aqi, aqi_cat, dom_poll, pm25, pm10, o3, no2, so2, co = row
        rows_by_hour[hour].append(
            {
                "timestamp": ts,
                "city": city,
                "state": state,
                "aqi": aqi,
                "aqi_category": aqi_cat,
                "dominant_pollutant": dom_poll,
                "pm25": pm25,
                "pm10": pm10,
                "o3": o3,
                "no2": no2,
                "so2": so2,
                "co": co,
            }
        )
    return rows_by_hour


def load_wx_rows(conn):
    """Return weather rows grouped by hour bucket (YYYY-MM-DD HH)."""
    cur = conn.execute(
        """
        SELECT
            strftime('%Y-%m-%d %H', collected_at) AS hour_bucket,
            collected_at,
            city,
            state,
            temperature_c,
            humidity_pct,
            wind_speed_kmh,
            wind_direction_deg,
            pressure_hpa,
            cloud_cover_pct,
            precipitation_mm
        FROM weather
        ORDER BY hour_bucket, collected_at
        """
    )
    rows_by_hour = defaultdict(list)
    for row in cur.fetchall():
        hour, ts, city, state, temp, hum, wind_spd, wind_dir, pres, cloud, precip = row
        rows_by_hour[hour].append(
            {
                "timestamp": ts,
                "city": city,
                "state": state,
                "temperature_c": temp,
                "humidity_pct": hum,
                "wind_speed_kmh": wind_spd,
                "wind_direction_deg": wind_dir,
                "pressure_hpa": pres,
                "cloud_cover_pct": cloud,
                "precipitation_mm": precip,
            }
        )
    return rows_by_hour


def main():
    producer = connect_kafka()

    conn = sqlite3.connect(DB_PATH)
    print(f"Opened SQLite DB at {DB_PATH}", flush=True)

    aq_by_hour = load_aq_rows(conn)
    wx_by_hour = load_wx_rows(conn)
    conn.close()

    all_hours = sorted(set(aq_by_hour.keys()) | set(wx_by_hour.keys()))

    if not all_hours:
        print("No data in DB, exiting.", flush=True)
        return

    print(f"Streaming {len(all_hours)} hours to Kafka...", flush=True)

    for hour in all_hours:
        aq_rows = aq_by_hour.get(hour, [])
        wx_rows = wx_by_hour.get(hour, [])

        for row in aq_rows:
            key = f"{row['city']}_{row['state']}"
            producer.send(AQ_TOPIC, key=key, value=row)

        for row in wx_rows:
            key = f"{row['city']}_{row['state']}"
            producer.send(WX_TOPIC, key=key, value=row)

        producer.flush()
        print(
            f"Sent hour={hour}: {len(aq_rows)} AQ rows, {len(wx_rows)} WX rows",
            flush=True,
        )
        time.sleep(HOUR_INTERVAL)

    producer.close()
    print("All hours streamed. Producer exiting.", flush=True)


if __name__ == "__main__":
    main()

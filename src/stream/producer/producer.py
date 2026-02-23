import json
import os
import sqlite3
import time
from collections import defaultdict

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka1:19092")
DB_PATH = os.environ.get("DB_PATH", "/data/measurements.db")
STREAM_INTERVAL = float(os.environ.get("STREAM_INTERVAL_SECONDS", "2"))
REPLAY_PAUSE = float(os.environ.get("REPLAY_PAUSE_SECONDS", "5"))

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
    """Return air quality rows grouped by collected_at timestamp."""
    cur = conn.execute(
        """
        SELECT
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
        ORDER BY collected_at
        """
    )
    rows_by_ts = defaultdict(list)
    for row in cur.fetchall():
        ts, city, state, aqi, aqi_cat, dom_poll, pm25, pm10, o3, no2, so2, co = row
        rows_by_ts[ts].append(
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
    return rows_by_ts


def load_wx_rows(conn):
    """Return weather rows grouped by collected_at timestamp."""
    cur = conn.execute(
        """
        SELECT
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
        ORDER BY collected_at
        """
    )
    rows_by_ts = defaultdict(list)
    for row in cur.fetchall():
        ts, city, state, temp, hum, wind_spd, wind_dir, pres, cloud, precip = row
        rows_by_ts[ts].append(
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
    return rows_by_ts


def main():
    producer = connect_kafka()

    conn = sqlite3.connect(DB_PATH)
    print(f"Opened SQLite DB at {DB_PATH}", flush=True)

    aq_by_ts = load_aq_rows(conn)
    wx_by_ts = load_wx_rows(conn)
    conn.close()

    all_timestamps = sorted(set(aq_by_ts.keys()) | set(wx_by_ts.keys()))

    if not all_timestamps:
        print("No data in DB, exiting.", flush=True)
        return

    print(f"Streaming {len(all_timestamps)} timestamps to Kafka (looping forever)...", flush=True)

    loop = 0
    while True:
        loop += 1
        print(f"--- Loop {loop}: replaying {len(all_timestamps)} timestamps ---", flush=True)
        for ts in all_timestamps:
            aq_rows = aq_by_ts.get(ts, [])
            wx_rows = wx_by_ts.get(ts, [])

            for row in aq_rows:
                key = f"{row['city']}_{row['state']}"
                producer.send(AQ_TOPIC, key=key, value=row)

            for row in wx_rows:
                key = f"{row['city']}_{row['state']}"
                producer.send(WX_TOPIC, key=key, value=row)

            producer.flush()
            print(
                f"Sent ts={ts}: {len(aq_rows)} AQ rows, {len(wx_rows)} WX rows",
                flush=True,
            )
            time.sleep(STREAM_INTERVAL)

        print(f"--- Loop {loop} complete. Pausing {REPLAY_PAUSE}s before next replay ---", flush=True)
        time.sleep(REPLAY_PAUSE)


if __name__ == "__main__":
    main()

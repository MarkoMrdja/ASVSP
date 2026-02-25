# -*- coding: utf-8 -*-
"""
Seed Hue with a single notebook containing all 10 batch queries as separate
pyspark snippets. One Livy session serves all queries - no per-query startup.

Run after creating a Hue user:
    docker exec hue python /scripts/seed_hue_queries.py <username>

Re-running is idempotent: existing notebook is updated in place.
"""

import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime

HUE_DB = "/usr/share/hue/desktop/desktop.db"
QUERIES_DIR = "/queries"
NOTEBOOK_NAME = "Air Quality Batch Queries"

QUERIES = [
    ("Q01 - State Ranking YoY",         "q01_state_ranking_yoy.sql"),
    ("Q02 - Peak Month",                "q02_peak_month.sql"),
    ("Q03 - Cumulative Exceedances",    "q03_cumulative_exceedances.sql"),
    ("Q04 - Month-over-Month",          "q04_month_over_month.sql"),
    ("Q05 - Same Month YoY",            "q05_same_month_yoy.sql"),
    ("Q06 - 30-Day Moving Average",     "q06_moving_average.sql"),
    ("Q07 - Percentile Classification", "q07_percentile_classification.sql"),
    ("Q08 - Covid Impact",              "q08_covid_impact.sql"),
    ("Q09 - Weekend Effect",            "q09_weekend_effect.sql"),
    ("Q10 - Consecutive Improvement",   "q10_consecutive_improvement.sql"),
]


def make_statement(sql):
    escaped = sql.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
    return 'spark.sql("""\n%s\n""").show(1000, truncate=False)' % escaped


def make_notebook_data(snippets):
    return json.dumps({
        "name": NOTEBOOK_NAME,
        "description": "All 10 US Air Quality batch queries. One session, no repeated startup cost.",
        "type": "notebook",
        "isSaved": True,
        "isManaged": False,
        "skipHistorify": False,
        "sessions": [],
        "snippets": snippets,
    })


def main():
    if len(sys.argv) < 2:
        print("Usage: python seed_hue_queries.py <hue_username>")
        sys.exit(1)

    username = sys.argv[1]

    conn = sqlite3.connect(HUE_DB)
    c = conn.cursor()

    c.execute("SELECT id FROM auth_user WHERE username = ?", (username,))
    row = c.fetchone()
    if not row:
        print("Error: user '%s' not found in Hue." % username)
        conn.close()
        sys.exit(1)
    owner_id = row[0]
    print("Found user '%s' (id=%d)" % (username, owner_id))

    # Build all snippets for the single combined notebook
    snippets = []
    for name, filename in QUERIES:
        sql_path = os.path.join(QUERIES_DIR, filename)
        with open(sql_path) as f:
            sql = f.read().strip()
        snippets.append({
            "id": str(uuid.uuid4()),
            "name": name,
            "type": "pyspark",
            "status": "ready",
            "statement_raw": make_statement(sql),
            "statement": make_statement(sql),
            "executor": "",
            "variables": [],
            "properties": {"files": [], "functions": [], "settings": []},
            "result": {},
            "lastExecuted": -1,
        })

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    data = make_notebook_data(snippets)

    # Find home directory id for this user
    c.execute(
        "SELECT id FROM desktop_document2 WHERE owner_id=? AND type='directory' AND name=''",
        (owner_id,),
    )
    home_row = c.fetchone()
    home_dir_id = home_row[0] if home_row else None

    # Remove old individual notebooks if they exist
    old_names = tuple([q[0] for q in QUERIES])
    placeholders = ",".join(["?" for _ in old_names])
    c.execute(
        "DELETE FROM desktop_document2 WHERE name IN (%s) AND owner_id = ? AND type = 'notebook'" % placeholders,
        list(old_names) + [owner_id],
    )
    deleted = c.rowcount
    if deleted:
        print("  Removed %d old individual query notebooks." % deleted)

    # Upsert the combined notebook
    c.execute(
        "SELECT id FROM desktop_document2 WHERE name = ? AND owner_id = ? AND type = 'notebook'",
        (NOTEBOOK_NAME, owner_id),
    )
    existing = c.fetchone()
    if existing:
        c.execute(
            "UPDATE desktop_document2 SET data = ?, last_modified = ? WHERE id = ?",
            (data, now, existing[0]),
        )
        print("  Updated combined notebook: '%s'" % NOTEBOOK_NAME)
    else:
        c.execute(
            """
            INSERT INTO desktop_document2
                (name, description, uuid, type, data, extra, last_modified,
                 version, is_history, owner_id, search, is_trashed, is_managed, connector_id, parent_directory_id)
            VALUES (?, '', ?, 'notebook', ?, '', ?, 1, 0, ?, '', 0, 0, NULL, ?)
            """,
            (NOTEBOOK_NAME, str(uuid.uuid4()), data, now, owner_id, home_dir_id),
        )
        print("  Created combined notebook: '%s'" % NOTEBOOK_NAME)

    conn.commit()
    conn.close()
    print("\nDone. Open '%s' in Hue Notebook to run all queries." % NOTEBOOK_NAME)


if __name__ == "__main__":
    main()

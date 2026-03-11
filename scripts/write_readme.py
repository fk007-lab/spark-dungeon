content = r"""# ⚡ Spark Dungeon

> **A self-hosted PySpark learning game built for MAANG-level Data Engineering interviews.**
> Dungeon crawl your way through real Spark challenges — with embedded theory, flashcards, quizzes, and a deep-dive reference library.

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apache-spark)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green?logo=fastapi)
![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker)

---

## 🎮 What Is This?

Spark Dungeon is a self-hosted, browser-based PySpark learning game that teaches you everything you need to pass senior Data Engineering interviews.

- **Two-tab concept panels** — every mission has a **Theory** tab (deep explanation, tables, callout boxes) and an **Example** tab (runnable code on *different* data so you learn the pattern, not the answer)
- **Rune Flashcards** — flip 3D cards to test yourself on key API terms before attempting the challenge
- **Oracle Quiz** — in-mission multiple-choice questions with instant feedback
- **Blank-based challenges** — scaffold code with `___` gaps; write real PySpark to fill them
- **Real datasets** — CSVs from the open-source PySpark community (Euro 2012, Titanic, Chipotle, US Census), not toy data
- **The Codex** — a 5-volume, 21-chapter reference library covering Spark internals, SQL optimisation, storage formats, streaming, and performance tuning
- **Dungeon Theory Scrolls** — each dungeon's mission list has a collapsible scroll summarising the whole topic
- **Dark fantasy D&D theme** — stone-wall background, torch-amber accents, Cinzel headings, flickering glow animations
- **Progress tracking** — solved missions persist in SQLite; dungeons unlock sequentially

---

## 🗺️ Dungeon Map

| # | Dungeon | Topic | Status |
|---|---------|-------|--------|
| 1 | **Cave of Awakening** | DataFrame basics — select, filter, withColumn, orderBy, rename | ✅ Complete (5 missions) |
| 2 | **Halls of Aggregation** | count, groupBy, agg (sum/avg), arithmetic columns, HAVING | ✅ Complete (5 missions) |
| 3 | **Tower of Windows** | ROW_NUMBER, RANK/DENSE_RANK, LAG/LEAD, running SUM, NTILE | ✅ Complete (5 missions) |
| 4 | **Bridge of Joins** | inner, left, outer, self joins | 📋 Planned |
| 5 | **Dungeon of Strings** | CASE/WHEN, UDFs, string ops, regex, PII masking | 📋 Planned |
| 6 | **Vault of Arrays** | collect_list, explode, split, collect_set, array functions | 📋 Planned |
| 7 | **Cursed Archives** | dropDuplicates, null handling, data quality, deduplication | 📋 Planned |
| 8 | **Citadel of Advanced SQL** | status changes, reconciliation, round-trip, market basket | 📋 Planned |
| 9 | **Temporal Towers** | date ops, unions, time series, stock prices | 📋 Planned |

---

## 📚 The Codex

A structured reference library readable any time, independent of mission progression. Access at **http://localhost:8000/codex**

| Volume | Topic | Chapters |
|--------|-------|----------|
| **I — Foundations** | Spark architecture, RDDs vs DataFrames, lazy evaluation, partitions, the shuffle | 5 |
| **II — Spark SQL & DataFrames** | Catalyst optimizer, reading query plans, join strategies, aggregations at scale, window functions | 5 |
| **III — Storage & Formats** | Parquet deep dive, Delta Lake & ACID, Z-ordering & data skipping, small file problem & compaction | 4 |
| **IV — Streaming & Kafka** | Structured Streaming, Spark + Kafka integration, stateful operations & watermarks | 3 |
| **V — Performance Engineering** | Adaptive Query Execution, skew handling, memory tuning, configuration cheatsheet | 4 |

---

## 🚀 Quick Start (Docker)

```bash
git clone https://github.com/fk007-lab/spark-dungeon.git
cd spark-dungeon

mkdir -p progress
docker compose up -d
```

Open **http://localhost:8000** in your browser.

> **Requirements:** Docker Desktop only — Java and PySpark run inside the container.

---

## 🏗️ Architecture

```
spark-dungeon/
├── backend/
│   ├── main.py            # FastAPI app + all routes
│   ├── spark_session.py   # Singleton SparkSession (local[2])
│   ├── mission_loader.py  # Loads & caches mission JSONs
│   ├── codex_loader.py    # Loads & caches Codex chapter JSONs
│   ├── runner.py          # Sandboxed PySpark code execution
│   ├── validator.py       # Compares student output to expected result
│   └── progression.py     # SQLite progress tracking
├── frontend/
│   ├── templates/
│   │   ├── base.html           # Layout shell (Cinzel font, CodeMirror, nav)
│   │   ├── dungeons.html       # Dungeon selection grid
│   │   ├── missions.html       # Mission list + collapsible theory scroll
│   │   ├── mission.html        # Two-tab concept panel + editor + result
│   │   ├── codex_index.html    # Codex table of contents (5 volumes)
│   │   └── codex_chapter.html  # Chapter reader (sections, rune cards, quiz)
│   └── static/
│       ├── css/style.css       # Full D&D dark theme + all component styles
│       └── js/editor.js        # CodeMirror init, run/hint/reset logic
├── data/
│   ├── datasets/          # Real CSVs (Titanic, Euro12, Chipotle, sales, etc.)
│   ├── codex/             # index.json + 21 chapter JSONs (v1c1–v5c4)
│   └── missions/
│       ├── dungeon1/      # meta.json + m1–m5.json (DataFrame basics)
│       ├── dungeon2/      # meta.json + m1–m5.json (Aggregations)
│       └── dungeon3/      # meta.json + m1–m5.json (Window Functions)
├── scripts/               # Bulk data-injection utility scripts
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
└── requirements.txt
```

### Mission JSON schema

A mission JSON includes both the challenge and all embedded learning content:

```json
{
  "id": "d1_m1",
  "dungeon_id": "dungeon1",
  "order": 1,
  "title": "First Light",
  "topic": "select()",
  "story": "Flavour text...",
  "problem": "What to do, written for humans",
  "dataset": "employees.csv",
  "concept": {
    "explanation": "HTML teaching the concept (Example tab)",
    "example_code": "# Code on DIFFERENT data — never the answer",
    "output_note": "Short note about output behaviour",
    "theory": [
      {"type": "text",    "content": "Deep explanation..."},
      {"type": "heading", "content": "Section heading"},
      {"type": "table",   "headers": ["Col A", "Col B"], "rows": [["val", "val"]]},
      {"type": "code",    "content": "df.select('col').show()"},
      {"type": "callout", "variant": "tip|info|warning", "content": "Pro tip..."}
    ],
    "rune_cards": [
      {"glyph": "◈", "term": "select()", "definition": "Projects columns from a DataFrame..."}
    ],
    "quiz": [
      {
        "question": "What does select() return?",
        "options": ["A new DataFrame", "A list", "None", "An RDD"],
        "answer": 0,
        "feedback": "select() always returns a new DataFrame."
      }
    ]
  },
  "starter_code": "result = df.select(___)",
  "expected": [{"col": "val"}],
  "hints": ["Hint 1", "Hint 2"]
}
```

### Codex chapter JSON schema

```json
{
  "id": "v1c1",
  "title": "Spark Architecture",
  "volume": "I",
  "estimated_time": "20 min",
  "sections": [
    {
      "title": "The Driver",
      "blocks": [
        {"type": "text",  "content": "The Driver is the JVM process..."},
        {"type": "code",  "content": "spark.sparkContext.getConf().getAll()"},
        {"type": "table", "headers": ["Component", "Role"], "rows": [["Driver", "Orchestrates the job"]]}
      ]
    }
  ],
  "rune_cards": [
    {"glyph": "⬡", "term": "DAG Scheduler", "definition": "Converts logical plan into stages..."}
  ],
  "quiz": [
    {
      "question": "Where does the Catalyst optimizer run?",
      "options": ["Executor", "Driver", "Cluster Manager", "Worker Node"],
      "answer": 1,
      "feedback": "Catalyst runs entirely on the Driver."
    }
  ]
}
```

---

## 🛠️ Local Dev (without Docker)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# Java required for PySpark
export JAVA_HOME=$(/usr/libexec/java_home 2>/dev/null || echo "/usr/lib/jvm/default-java")

uvicorn backend.main:app --reload --port 8000
```

---

## 📦 Datasets Used

| File | Source | Used In |
|------|--------|---------|
| `employees.csv` | Hand-crafted | Dungeons 1, 2 |
| `sales.csv` | Hand-crafted | Dungeon 3 |
| `euro12.csv` | [pyspark_exercises](https://github.com/areibman/pyspark_exercises) | Dungeon 4+ |
| `titanic.csv` | [pyspark_exercises](https://github.com/areibman/pyspark_exercises) | Dungeon 7+ |
| `chipotle.tsv` | [justmarkham/DAT8](https://github.com/justmarkham/DAT8) | Dungeon 9 |
| `apple_stock.csv` | [pyspark_exercises](https://github.com/areibman/pyspark_exercises) | Dungeon 9 |
| `tips.csv` / `us_crime_rates.csv` | [pyspark_exercises](https://github.com/areibman/pyspark_exercises) | Dungeons 5–8 |

---

## 🤝 Contributing

**Adding a mission:** Create `data/missions/dungeonN/mK.json` using the schema above. Add the concept explanation on *different* data, set `starter_code` with `___` gaps, and list `expected` rows. Changes are live instantly via Docker volume mount — no rebuild needed.

**Adding a Codex chapter:** Create `data/codex/{id}.json` using the chapter schema, then register the entry in `data/codex/index.json`.

---

## License

MIT
"""

with open("README.md", "w") as f:
    f.write(content)

print("README.md written successfully.")

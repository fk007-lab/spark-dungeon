# ⚡ Spark Dungeon

> **Learn PySpark by playing a dark-fantasy dungeon crawler.**  
> Each dungeon is a themed world. Each mission is a real PySpark challenge.  
> Read the concept, try the example, then solve the challenge yourself.

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apache-spark)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green?logo=fastapi)
![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker)

---

## 🎮 What Is This?

Spark Dungeon is a self-hosted, browser-based PySpark learning game.

- **W3Schools-style concept panels** — every mission opens with an explanation of the concept + a runnable code example on *different* data so you learn the pattern without seeing the answer
- **Blank-based challenges** — the editor gives you scaffold code with `___` gaps to fill in yourself
- **Real datasets** — actual CSVs from the open-source PySpark community (Euro 2012, Titanic, Chipotle, US Census), not toy data
- **Dark fantasy D&D theme** — stone-wall background, torch-amber accents, Cinzel headings, flickering glow animations
- **Progress tracking** — solved missions persist in SQLite; dungeons unlock sequentially

---

## 🗺️ Dungeon Map

| # | Dungeon | Topic | Status |
|---|---------|-------|--------|
| 1 | **Cave of Awakening** | DataFrame basics — select, filter, withColumn, orderBy, rename | ✅ Complete |
| 2 | **Halls of Aggregation** | count, groupBy, agg (sum/avg), arithmetic columns, HAVING | ✅ Complete |
| 3 | **Tower of Windows** | ROW_NUMBER, RANK/DENSE_RANK, cumulative SUM, LAG/LEAD, 2nd highest | 🔨 In Progress |
| 4 | **Bridge of Joins** | inner, left, outer, self joins | 📋 Planned |
| 5 | **Dungeon of Strings** | CASE/WHEN, UDFs, string ops, regex, PII masking | 📋 Planned |
| 6 | **Vault of Arrays** | collect_list, explode, split, collect_set, array functions | 📋 Planned |
| 7 | **Cursed Archives** | dropDuplicates, null handling, data quality, deduplication | 📋 Planned |
| 8 | **Citadel of Advanced SQL** | status changes, reconciliation, round-trip, market basket | 📋 Planned |
| 9 | **Temporal Towers** | date ops, unions, time series, stock prices | 📋 Planned |

---

## 🚀 Quick Start (Docker)

```bash
git clone https://github.com/fk007-lab/spark-dungeon.git
cd spark-dungeon

mkdir -p progress
docker compose up -d
```

Open **http://localhost:8000** in your browser.

> **Requirements:** Docker Desktop. Nothing else — Java and PySpark run inside the container.

---

## 🏗️ Architecture

```
spark-dungeon/
├── backend/
│   ├── main.py            # FastAPI app + routes
│   ├── spark_session.py   # Singleton SparkSession (local[2])
│   ├── mission_loader.py  # Loads & caches mission JSONs from data/missions/
│   ├── runner.py          # Sandboxed PySpark code execution
│   ├── validator.py       # Compares student output to expected result
│   └── progression.py     # SQLite progress tracking
├── frontend/
│   ├── templates/
│   │   ├── base.html      # Layout shell (Cinzel font, CodeMirror)
│   │   ├── dungeons.html  # Dungeon selection grid
│   │   ├── missions.html  # Mission list per dungeon
│   │   └── mission.html   # Concept panel + editor + result
│   └── static/
│       ├── css/style.css  # Full D&D dark theme
│       └── js/editor.js   # CodeMirror init, run/hint/reset logic
├── data/
│   ├── datasets/          # Real CSVs (Titanic, Euro12, Chipotle, etc.)
│   └── missions/
│       ├── dungeon1/      # meta.json + m1–m5.json
│       ├── dungeon2/      # meta.json + m1–m5.json
│       └── dungeon3/      # meta.json + m1.json (more WIP)
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
└── requirements.txt
```

### Mission JSON schema

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
    "explanation": "HTML string with <code>tags</code> teaching the concept",
    "example_code": "# Python on DIFFERENT data than the challenge\nfruits_df.select('name', 'price')",
    "output_note": "Short note about output behaviour"
  },
  "starter_code": "result = df.select(___)",
  "expected": [{"col": "val"}, ...],
  "hints": ["Hint 1", "Hint 2"]
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
| `employees.csv` | Hand-crafted | Dungeons 1, 2, 4 |
| `sales.csv` | Hand-crafted | Dungeon 3 |
| `euro12.csv` | [pyspark_exercises](https://github.com/areibman/pyspark_exercises) | Dungeon 4+ |
| `titanic.csv` | [pyspark_exercises](https://github.com/areibman/pyspark_exercises) | Dungeon 7+ |
| `chipotle.tsv` | [justmarkham/DAT8](https://github.com/justmarkham/DAT8) | Dungeon 9 |
| `apple_stock.csv` | [pyspark_exercises](https://github.com/areibman/pyspark_exercises) | Dungeon 9 |
| `tips.csv` / `us_crime_rates.csv` | [pyspark_exercises](https://github.com/areibman/pyspark_exercises) | Dungeons 5–8 |

---

## 🤝 Contributing

Missions are plain JSON files in `data/missions/`. To add a mission:

1. Copy the schema above into a new `mN.json` in the right dungeon folder
2. Add a concept explanation with an example on *different* data
3. Set `starter_code` to a scaffold with `___` gaps — **never the answer**
4. List `expected` output rows for auto-validation

---

## License

MIT

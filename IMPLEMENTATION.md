# Spark Dungeon — Implementation Plan

> Status key: `[x]` done · `[~]` in progress · `[ ]` not started

---

## Phase 1 — Core Infrastructure (COMPLETE)

- [x] Project folder structure (`backend/`, `data/`, `frontend/`, `progress/`)
- [x] `requirements.txt` (fastapi, uvicorn, pyspark, jinja2, aiofiles)
- [x] `backend/spark_session.py` — singleton SparkSession local[2]
- [x] `backend/main.py` — FastAPI app, lifespan hook, Jinja2 + StaticFiles
- [x] GET `/` renders dungeons.html
- [x] GET `/dungeon/{dungeon_id}` renders missions.html
- [x] GET `/mission/{mission_id}` renders mission.html
- [x] POST `/mission/{mission_id}/run` — execute + validate + mark solved
- [x] `frontend/templates/base.html` — layout shell (Cinzel font, CodeMirror)
- [x] `frontend/templates/dungeons.html` — dungeon grid with progress bars
- [x] `frontend/templates/missions.html` — mission list (locked/unlocked/solved)
- [x] `frontend/templates/mission.html` — concept panel + editor + result
- [x] `frontend/static/css/style.css` — D&D dark fantasy theme
- [x] `frontend/static/js/editor.js` — CodeMirror init, run/hint/reset

---

## Phase 2 — Data Layer (COMPLETE)

- [x] employees.csv, departments.csv, sales.csv, orders.csv, customers.csv
- [x] sensor.csv, user_pages.csv, products.csv, students.csv
- [x] corrupt_employees.csv, source.csv, target.csv, family.csv, country.csv
- [x] multi_source1.csv, multi_source2.csv, time_series.csv
- [x] Real datasets: euro12.csv, titanic.csv, tips.csv, apple_stock.csv
- [x] Real datasets: us_crime_rates.csv, us_baby_names.csv, student_alcohol.csv
- [x] Real datasets: cars1.csv, cars2.csv, chipotle.tsv, u_user.dat, scenerio25.csv

---

## Phase 3 — Runner + Validator (COMPLETE)

- [x] `backend/runner.py` — sandboxed exec() with restricted namespace (df, spark, F, Window)
- [x] `backend/validator.py` — collect + sort + compare; returns {passed, hint}
- [x] `backend/mission_loader.py` — scan + cache all JSONs, load Spark DataFrame per mission
- [x] `backend/progression.py` — SQLite: init DB, get_status, mark_solved, unlock_next, reset

---

## Phase 4 — W3Schools-Style Concept Teaching (COMPLETE)

- [x] Concept panel above the two-column layout on every mission
- [x] Explanation text with inline code highlighting
- [x] Code example on DIFFERENT data than the challenge
- [x] Try it in the editor button — loads example into CodeMirror
- [x] Reset button — restores scaffold starter code
- [x] starter_code uses ___ blanks — solution is never shown
- [x] Show Hint button

---

## Phase 5 — Docker Deployment (COMPLETE)

- [x] Dockerfile — python:3.12-slim + Java 21 (default-jre-headless)
- [x] entrypoint.sh — dynamic JAVA_HOME detection
- [x] docker-compose.yml — ports, volumes, env vars
- [x] frontend/ mounted as live volume (CSS/template changes reflect without rebuild)
- [x] data/ mounted as live volume (mission JSON edits reflect without rebuild)

---

## Phase 6 — Dungeons 1 and 2 (COMPLETE)

### Dungeon 1 — Cave of Awakening (DataFrame Basics)
- [x] m1 — select() — choose columns
- [x] m2 — filter() — filter rows
- [x] m3 — withColumn() — derive new column
- [x] m4 — orderBy() — sort results
- [x] m5 — withColumnRenamed() — rename column

### Dungeon 2 — Halls of Aggregation
- [x] m1 — count() with alias
- [x] m2 — groupBy().count()
- [x] m3 — agg(sum, avg)
- [x] m4 — column arithmetic
- [x] m5 — HAVING equivalent (filter on aggregated value)

---

## Phase 7 — Dungeons 3-9 (IN PROGRESS)

### Dungeon 3 — Tower of Windows (Window Functions)
- [x] m1 — ROW_NUMBER() partitioned by region
- [ ] m2 — RANK() vs DENSE_RANK() on tied values
- [ ] m3 — cumulative SUM() running total
- [ ] m4 — LAG() / LEAD() year-over-year
- [ ] m5 — 2nd highest salary per department

### Dungeon 4 — Bridge of Joins
- [ ] m1 — inner join
- [ ] m2 — left join
- [ ] m3 — full outer join / source-target diff
- [ ] m4 — cross join
- [ ] m5 — self join

### Dungeon 5 — Dungeon of Strings
- [ ] m1 — CASE WHEN designation
- [ ] m2 — salary grade bucketing
- [ ] m3 — PII masking (email + mobile)
- [ ] m4 — reverse words in string column
- [ ] m5 — age range bucketing

### Dungeon 6 — Vault of Arrays
- [ ] m1 — collect_set() unique values
- [ ] m2 — collect_list() page trail
- [ ] m3 — array rank-1 filter
- [ ] m4 — explode() + split()
- [ ] m5 — sell_date collect_set + size()

### Dungeon 7 — Cursed Archives (Data Quality)
- [ ] m1 — dropDuplicates()
- [ ] m2 — DROPMALFORMED bad records
- [ ] m3 — count nulls per column
- [ ] m4 — fill null with column mean
- [ ] m5 — drop sparse columns/rows

### Dungeon 8 — Citadel of Advanced SQL
- [ ] m1 — status change dates (lag + filter)
- [ ] m2 — sensor row-to-row differences
- [ ] m3 — source-target reconciliation
- [ ] m4 — symmetric difference
- [ ] m5 — range join

### Dungeon 9 — Temporal Towers (Dates + Unions)
- [ ] m1 — union() two DataFrames
- [ ] m2 — add constant column then union
- [ ] m3 — filter valid emails (regex)
- [ ] m4 — partitionBy write concept
- [ ] m5 — date arithmetic (extract month/year, add days)

---

## Phase 8 — Polish (AFTER all 9 dungeons)

- [ ] Ctrl+Enter / Cmd+Enter keyboard shortcut to run
- [ ] Hint cycling on repeated failures
- [ ] Confetti / XP animation on mission complete
- [ ] Dungeon completion story epilogue screen
- [ ] Mobile responsive layout
- [ ] Loading spinner during PySpark execution

---

## Deferred / Post-MVP

- [ ] User accounts / leaderboard
- [ ] Timed challenge mode
- [ ] Spark SQL mode (write SQL instead of Python API)
- [ ] Structured Streaming basics dungeon
- [ ] Export progress as PDF certificate

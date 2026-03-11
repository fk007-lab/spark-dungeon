"""
Add theory/rune_cards/quiz fields to all D1 and D2 mission JSONs.
Run from project root: python scripts/add_theory.py
"""
import json, pathlib, sys

BASE = pathlib.Path(__file__).parent.parent / "data" / "missions"

MISSIONS = {
    "dungeon1/m1.json": {
        "theory": [
            {"type": "text", "content": "<strong>select()</strong> is the most fundamental DataFrame operation. It projects a subset of columns — equivalent to <code>SELECT col1, col2 FROM table</code> in SQL. Under the hood Spark applies <em>column pruning</em> at the physical plan level, meaning only the columns you request are read from disk (Parquet or ORC skip the rest entirely)."},
            {"type": "heading", "content": "Three ways to select columns"},
            {"type": "table",
             "headers": ["Syntax", "When to use", "Example"],
             "rows": [
                 ["<code>df.select('name', 'salary')</code>", "Simple column names (string)", "Most common, readable"],
                 ["<code>df.select(df.name, df.salary)</code>", "Column attribute (avoids typos)", "Useful in IDE with autocomplete"],
                 ["<code>df.select(col('name'), col('salary'))</code>", "Complex expressions, renaming, functions", "<code>col('salary') * 1.1</code>"]
             ]},
            {"type": "callout", "style": "warning", "icon": "⚠️", "content": "In PySpark <strong>column names are case-sensitive by default</strong> for programmatic access (<code>col()</code> / attribute), but Spark SQL is case-insensitive. Always confirm source column names with <code>df.printSchema()</code>."},
            {"type": "code", "content": "# Column pruning example — Parquet skips unneeded columns\ndf = spark.read.parquet('s3://bucket/employees/')\n# Only 'name' and 'salary' row-groups are read from disk:\nresult = df.select('name', 'salary')\n\n# You can also pass a Python list:\ncols = ['name', 'salary']\nresult = df.select(*cols)"},
            {"type": "callout", "style": "tip", "icon": "💡", "content": "At MAANG scale, column pruning via <code>select()</code> early in your pipeline can reduce shuffle bytes and executor memory pressure dramatically. Always project down before joining or aggregating."}
        ],
        "rune_cards": [
            {"term": "Column Pruning", "definition": "Spark's optimizer removes columns not needed downstream. Triggered by <code>select()</code> — critical for wide Parquet tables."},
            {"term": "Projection", "definition": "The relational algebra term for selecting a subset of columns. <code>select()</code> compiles to a Projection node in the logical plan."},
            {"term": "col()", "definition": "Factory function from <code>pyspark.sql.functions</code> that creates a <code>Column</code> object. Needed for expressions: <code>col('price') * 1.2</code>."},
            {"term": "Catalyst Optimizer", "definition": "Spark's query optimizer that rewrites your logical plan into an optimized physical plan — column pruning, predicate pushdown, constant folding."}
        ],
        "quiz": [
            {
                "type": "mcq",
                "question": "You have a Parquet table with 80 columns. You run <code>df.select('user_id', 'event_type')</code> on a Spark cluster. What actually happens at the storage layer?",
                "options": [
                    "All 80 columns are read from disk, then the other 78 are dropped in memory",
                    "Only the 2 requested columns are read from Parquet row groups, the rest are skipped entirely",
                    "Spark reads a random sample of columns to estimate statistics",
                    "The query fails unless you also call .cache() first"
                ],
                "answer": 1,
                "explanation": "Parquet is a columnar format. Spark's Catalyst optimizer pushes column pruning down to the reader — only the requested columns' row groups are deserialized. This is one of the biggest perf wins when working with wide tables."
            },
            {
                "type": "spot_the_bug",
                "question": "This code should select <code>name</code> and <code>salary</code> but crashes. Find the bug:",
                "code": "from pyspark.sql.functions import col\nresult = df.select(col['name'], col['salary'])",
                "options": [
                    "col() should be imported from pyspark.sql instead",
                    "col() is a function — use col('name'), not col['name'] (no square brackets)",
                    "You must use df.name attribute syntax, col() can't be used here",
                    "select() only accepts string literals, not col() objects"
                ],
                "answer": 1,
                "explanation": "<code>col</code> is a function, not a dict. Use <code>col('name')</code> with parentheses. Square brackets would raise <code>TypeError: 'function' object is not subscriptable</code>."
            }
        ]
    },
    "dungeon1/m2.json": {
        "theory": [
            {"type": "text", "content": "<strong>filter()</strong> (alias: <strong>where()</strong>) applies row-level predicates — equivalent to SQL <code>WHERE</code>. In Spark's execution model, filter predicates are pushed as early as possible in the DAG (<em>predicate pushdown</em>) and, for Parquet/ORC, are pushed all the way to the file reader to skip entire row groups."},
            {"type": "heading", "content": "Predicate syntax options"},
            {"type": "table",
             "headers": ["Style", "Code", "Notes"],
             "rows": [
                 ["SQL string", "<code>df.filter(\"salary > 80000\")</code>", "Easiest to read; parsed by Spark SQL parser"],
                 ["Column expression", "<code>df.filter(col('salary') > 80000)</code>", "Type-safe; composable with & | ~"],
                 ["Attribute", "<code>df.filter(df.salary > 80000)</code>", "Short; beware of ambiguity across joins"],
                 ["AND / OR", "<code>df.filter((col('dept') == 'Eng') & (col('salary') > 80000))</code>", "Always wrap each condition in parentheses!"]
             ]},
            {"type": "callout", "style": "warning", "icon": "⚠️", "content": "A common bug: <strong>using Python <code>and</code>/<code>or</code> instead of <code>&</code>/<code>|</code></strong>. Python's <code>and</code> on Column objects doesn't do element-wise comparison — it raises a <code>ValueError</code>. Always use <code>&</code> for AND and <code>|</code> for OR, and wrap each sub-expression in parentheses."},
            {"type": "code", "content": "# CORRECT: bitwise operators, parentheses\nresult = df.filter((col('dept') == 'Engineering') & (col('salary') > 80000))\n\n# WRONG: Python 'and' raises ValueError\n# result = df.filter(col('dept') == 'Engineering' and col('salary') > 80000)\n\n# Parquet predicate pushdown — Spark pushes salary > 80000\n# down to the file reader to skip entire row groups\ndf_parquet = spark.read.parquet('s3://bucket/employees/')\nresult = df_parquet.filter(col('salary') > 80000)"},
            {"type": "callout", "style": "tip", "icon": "💡", "content": "At MAANG scale, place <code>filter()</code> as early as possible in your pipeline — before joins, groupBys, and window functions. Reducing row count early shrinks shuffle data and executor memory pressure."}
        ],
        "rune_cards": [
            {"term": "Predicate Pushdown", "definition": "Catalyst optimization that moves filter conditions as close to the data source as possible — all the way to Parquet/ORC row-group statistics, skipping unneeded blocks without reading them."},
            {"term": "Row Group", "definition": "Parquet's horizontal partition (default 128 MB). Each row group stores min/max statistics per column. Filter predicates use these stats to skip entire groups."},
            {"term": "& | ~ operators", "definition": "PySpark's element-wise boolean operators for Column objects. Use <code>&</code> (AND), <code>|</code> (OR), <code>~</code> (NOT). Never use Python's <code>and</code>/<code>or</code>/<code>not</code>."},
            {"term": "where() alias", "definition": "<code>df.where()</code> is 100% identical to <code>df.filter()</code>. Both compile to the same logical plan node. Use whichever reads more like SQL to your team."}
        ],
        "quiz": [
            {
                "type": "mcq",
                "question": "Which of the following correctly filters rows where <code>dept</code> is <code>'Engineering'</code> AND <code>salary</code> is above 90000?",
                "options": [
                    "<code>df.filter(col('dept') == 'Engineering' and col('salary') > 90000)</code>",
                    "<code>df.filter((col('dept') == 'Engineering') & (col('salary') > 90000))</code>",
                    "<code>df.filter(col('dept') == 'Engineering').filter(col('salary') > 90000)</code>",
                    "Both B and C are correct"
                ],
                "answer": 3,
                "explanation": "Both B and C are valid. B uses a compound predicate in one filter (Catalyst will combine them). C chains two separate filter() calls, which Spark collapses into the same physical predicate. A is <em>wrong</em> — Python's <code>and</code> can't operate on Column objects."
            },
            {
                "type": "spot_the_bug",
                "question": "A senior engineer wrote this to filter active high-earners. Why is it wrong?",
                "code": "result = df.filter(col('active') == True and col('salary') > 100000)",
                "options": [
                    "col('active') == True should be col('active').isTrue()",
                    "Python's 'and' cannot be used with Column objects — raises ValueError",
                    "The salary threshold must be a float: 100000.0",
                    "filter() doesn't accept boolean columns directly"
                ],
                "answer": 1,
                "explanation": "Python's <code>and</code> tries to evaluate both Column objects as Python booleans, which raises <code>ValueError: Cannot convert column into bool</code>. Use <code>&</code>: <code>df.filter((col('active') == True) & (col('salary') > 100000))</code>"
            }
        ]
    },
    "dungeon1/m3.json": {
        "theory": [
            {"type": "text", "content": "<strong>withColumn()</strong> adds or replaces a column in a DataFrame. It takes a column name and a <code>Column</code> expression. The expression can reference existing columns via <code>col()</code>, use built-in functions, or perform arithmetic. <strong>Important</strong>: every <code>withColumn()</code> adds a <em>new projection node</em> to the logical plan — chaining many withColumn() calls generates deeply nested plans that slow down query compilation."},
            {"type": "heading", "content": "withColumn vs select for multiple new columns"},
            {"type": "table",
             "headers": ["Approach", "Plan nodes added", "Compile time"],
             "rows": [
                 ["<code>df.withColumn('a', ...).withColumn('b', ...).withColumn('c', ...)</code>", "3 projection nodes", "Grows O(n) — avoid at scale"],
                 ["<code>df.select('*', expr1.alias('a'), expr2.alias('b'), expr3.alias('c'))</code>", "1 projection node", "Flat plan — preferred for many cols"]
             ]},
            {"type": "code", "content": "from pyspark.sql.functions import col, round, lit, when\n\n# Add a bonus column (10% of salary)\nresult = df.withColumn('bonus', col('salary') * 0.10)\n\n# Replace an existing column (round salary to nearest 1000)\nresult = df.withColumn('salary', (round(col('salary') / 1000) * 1000).cast('int'))\n\n# Performance-safe: use select for multiple new cols\nresult = df.select(\n    '*',\n    (col('salary') * 0.10).alias('bonus'),\n    (col('salary') * 1.12).alias('salary_with_tax'),\n    lit('USD').alias('currency')\n)"},
            {"type": "callout", "style": "tip", "icon": "💡", "content": "The <code>lit()</code> function creates a <strong>literal column</strong> (constant value). Use it when you need to add a fixed string, number, or boolean as a column: <code>lit('USD')</code>, <code>lit(True)</code>, <code>lit(0)</code>."},
            {"type": "callout", "style": "warning", "icon": "⚠️", "content": "<strong>Never chain 20+ withColumn() calls.</strong> Each generates a new Project node. For 50 new columns, Spark's analyzer must traverse a 50-deep plan tree. Use a single <code>select()</code> instead — the MAANG standard."}
        ],
        "rune_cards": [
            {"term": "withColumn()", "definition": "Adds or overwrites a column: <code>df.withColumn('col_name', expression)</code>. Overwrites if the name already exists. One projection node per call."},
            {"term": "col()", "definition": "References a DataFrame column as an expression object. Needed for arithmetic, comparisons, and function calls: <code>col('salary') * 1.1</code>."},
            {"term": "lit()", "definition": "Creates a constant column: <code>lit('USD')</code>. Import from <code>pyspark.sql.functions</code>. Without it, passing a Python scalar throws a TypeError."},
            {"term": "alias()", "definition": "Renames a column expression: <code>col('salary').alias('annual_pay')</code>. Same as SQL <code>AS</code>. Used in <code>select()</code> and <code>agg()</code>."}
        ],
        "quiz": [
            {
                "type": "mcq",
                "question": "You need to add 15 new computed columns to a DataFrame. Which approach is better at MAANG scale?",
                "options": [
                    "Chain 15 <code>withColumn()</code> calls — Spark automatically optimizes this",
                    "Use a single <code>select('*', expr1.alias('c1'), ...)</code> call with all expressions",
                    "Use <code>df.cache()</code> between every <code>withColumn()</code> to reduce plan depth",
                    "Both approaches produce identical query plans after Catalyst optimization"
                ],
                "answer": 1,
                "explanation": "Each <code>withColumn()</code> adds a Project node to the logical plan. 15 chained calls = 15 nested nodes. Catalyst does NOT flatten them automatically in all versions. A single <code>select()</code> with all expressions = 1 flat Project node. Faster to compile, clearer to debug."
            },
            {
                "type": "spot_the_bug",
                "question": "This code tries to add a constant <code>currency</code> column but fails. Why?",
                "code": "result = df.withColumn('currency', 'USD')",
                "options": [
                    "withColumn() requires .alias() on every expression",
                    "A Python string literal can't be used directly — wrap it with lit(): withColumn('currency', lit('USD'))",
                    "The column name 'currency' conflicts with a reserved SQL keyword",
                    "withColumn() only accepts col() expressions, not any other type"
                ],
                "answer": 1,
                "explanation": "PySpark expects a <code>Column</code> object as the second argument to <code>withColumn()</code>. A bare Python string <code>'USD'</code> is not a Column — it raises <code>AnalysisException</code>. Use <code>lit('USD')</code> to create a literal Column."
            }
        ]
    },
    "dungeon1/m4.json": {
        "theory": [
            {"type": "text", "content": "<strong>groupBy()</strong> partitions rows into groups by one or more columns, then <strong>agg()</strong> computes aggregate functions over each group — identical to SQL <code>GROUP BY</code>. In Spark's physical model, a <em>HashAggregate</em> runs in two phases: a partial aggregation on each executor (map side) followed by a final merge after a shuffle."},
            {"type": "heading", "content": "Aggregation functions (import from pyspark.sql.functions)"},
            {"type": "table",
             "headers": ["Function", "Description", "SQL equivalent"],
             "rows": [
                 ["<code>count('*')</code>", "Count all rows in group", "<code>COUNT(*)</code>"],
                 ["<code>sum('salary')</code>", "Sum of a numeric column", "<code>SUM(salary)</code>"],
                 ["<code>avg('salary')</code>", "Mean value", "<code>AVG(salary)</code>"],
                 ["<code>min() / max()</code>", "Min / max in group", "<code>MIN / MAX</code>"],
                 ["<code>countDistinct('id')</code>", "Exact distinct count", "<code>COUNT(DISTINCT id)</code>"],
                 ["<code>approx_count_distinct('id')</code>", "HyperLogLog estimate (~5% error)", "No SQL equivalent — much faster"]
             ]},
            {"type": "code", "content": "from pyspark.sql.functions import count, avg, sum, max, min, countDistinct\n\n# employees per department\nresult = df.groupBy('department').agg(\n    count('*').alias('headcount'),\n    avg('salary').alias('avg_salary'),\n    max('salary').alias('max_salary')\n)\n\n# Multi-column groupBy\nresult = df.groupBy('department', 'job_level').agg(\n    sum('salary').alias('total_payroll'),\n    countDistinct('employee_id').alias('unique_employees')\n)"},
            {"type": "callout", "style": "warning", "icon": "⚠️", "content": "<strong>countDistinct() triggers a full shuffle</strong> because Spark must collect all distinct values globally. At MAANG scale (billions of rows) use <code>approx_count_distinct()</code> with HyperLogLog — ~5% error but 10-100× faster with no shuffle overhead."},
            {"type": "callout", "style": "info", "icon": "ℹ️", "content": "<strong>Two-phase aggregation:</strong> Spark runs a partial aggregate per partition (executor-local, no network), then shuffles partition results to a final aggregate node. The intermediate shuffle only carries aggregated data per group — vastly smaller than shuffling raw rows."}
        ],
        "rune_cards": [
            {"term": "HashAggregate", "definition": "Spark's physical aggregate operator. Uses a hash table to group rows in memory. Two phases: partial (per partition) → final (after shuffle merge)."},
            {"term": "Partial Aggregate", "definition": "First phase of groupBy: each executor pre-aggregates its own partition before shuffling. Dramatically reduces shuffle data."},
            {"term": "countDistinct()", "definition": "Exact distinct count — requires full shuffle of keys. Use <code>approx_count_distinct()</code> + HyperLogLog when exact precision isn't required (MAANG default for dashboards)."},
            {"term": "Shuffle", "definition": "Network transfer of rows between executors to co-locate records with the same group key. The most expensive operation in Spark — minimize shuffle count and size."}
        ],
        "quiz": [
            {
                "type": "mcq",
                "question": "You're building a real-time dashboard that needs to count distinct users per country across 2 billion events. Which approach is best?",
                "options": [
                    "<code>df.groupBy('country').agg(countDistinct('user_id'))</code> — guarantees accuracy",
                    "<code>df.groupBy('country').agg(approx_count_distinct('user_id', 0.05))</code> — HyperLogLog with ~5% error",
                    "<code>df.groupBy('country').agg(sum('user_id'))</code> — sum is faster than count",
                    "<code>df.select('country','user_id').distinct().groupBy('country').count()</code>"
                ],
                "answer": 1,
                "explanation": "<code>approx_count_distinct()</code> uses HyperLogLog++ — O(1) memory per group, no extra shuffle, &lt;5% error. At 2B events, exact <code>countDistinct</code> requires a full shuffle of all user IDs and may OOM. For dashboards, ~5% error is acceptable. Option D forces a full dedup shuffle before aggregating — worst of both worlds."
            },
            {
                "type": "spot_the_bug",
                "question": "This code should count employees per department but produces a confusing error:",
                "code": "from pyspark.sql.functions import count\nresult = df.groupBy('department').count('*')",
                "options": [
                    "count('*') should be count() with no arguments",
                    ".count() is a DataFrame shorthand — it counts all rows in the whole DataFrame, not per group. Use .agg(count('*')) after groupBy",
                    "The string '*' is invalid in PySpark — use count(lit(1)) instead",
                    "groupBy() requires .agg() before any column can be selected"
                ],
                "answer": 1,
                "explanation": "<code>df.groupBy('dept').count()</code> (no args) is actually valid PySpark shorthand and does count per group. But <code>.count('*')</code> with an argument is wrong — the shorthand <code>count()</code> method on a RelationalGroupedDataset doesn't take arguments. Use <code>.agg(count('*').alias('n'))</code> for full control."
            }
        ]
    },
    "dungeon1/m5.json": {
        "theory": [
            {"type": "text", "content": "<strong>orderBy()</strong> (alias: <strong>sort()</strong>) sorts the entire DataFrame by one or more columns. In Spark's distributed execution model, a global sort requires: (1) a sample pass to compute partition boundaries, (2) a range partition shuffle to route rows, (3) a local sort within each partition. This is <em>expensive</em> — avoid unnecessary sorts."},
            {"type": "heading", "content": "Sort control"},
            {"type": "table",
             "headers": ["Syntax", "Order", "Notes"],
             "rows": [
                 ["<code>df.orderBy('salary')</code>", "Ascending (default)", "Single column"],
                 ["<code>df.orderBy(col('salary').desc())</code>", "Descending", "Use <code>.desc()</code> method"],
                 ["<code>df.orderBy(col('dept').asc(), col('salary').desc())</code>", "Multi-column", "Primary sort then secondary"],
                 ["<code>df.orderBy(col('salary').desc_nulls_last())</code>", "NULLs at end", "Control NULL placement"]
             ]},
            {"type": "code", "content": "from pyspark.sql.functions import col, desc\n\n# Top 5 earners\ntop5 = df.orderBy(col('salary').desc()).limit(5)\n\n# Sort by dept asc, then salary desc within dept\nresult = df.orderBy(col('department').asc(), col('salary').desc())\n\n# Equivalent: using desc() function instead of .desc() method\nresult = df.orderBy(desc('salary'))"},
            {"type": "callout", "style": "warning", "icon": "⚠️", "content": "<strong>Global sort forces a full shuffle.</strong> In production pipelines, avoid <code>orderBy()</code> unless you truly need a globally sorted output file. For 'top N' patterns always pair with <code>.limit(N)</code> so Spark can optimize away most data movement. If you only need sorted output per partition, use <code>sortWithinPartitions()</code> — zero shuffle cost."},
            {"type": "callout", "style": "tip", "icon": "💡", "content": "<strong>At MAANG scale</strong>, the typical pattern is: early filter → groupBy/agg → <code>orderBy(...).limit(N)</code> for ranking. Adding <code>.limit(N)</code> after <code>orderBy()</code> lets Catalyst push the limit up and avoid a full global sort of every partition."}
        ],
        "rune_cards": [
            {"term": "Range Partition", "definition": "The shuffle type used by <code>orderBy()</code>. Spark samples the data to estimate value distribution, then assigns each row to a partition covering a value range — ensuring globally sorted output."},
            {"term": "sortWithinPartitions()", "definition": "Sorts rows within each partition locally — zero shuffle cost. Use when downstream consumers process one partition at a time (e.g., writing sorted Parquet files)."},
            {"term": ".desc() / .asc()", "definition": "Column methods to control sort order. <code>col('salary').desc()</code> sorts largest first. <code>.desc_nulls_last()</code> / <code>.asc_nulls_first()</code> control NULL placement."},
            {"term": "limit(N)", "definition": "Truncates output to N rows. When combined with <code>orderBy()</code>, Catalyst may optimize to a top-K algorithm (heap sort) that avoids a full sort of all partitions."}
        ],
        "quiz": [
            {
                "type": "mcq",
                "question": "You need the top 10 highest-paid employees from a 500 million row DataFrame. Which is the most performant?",
                "options": [
                    "<code>df.orderBy(col('salary').desc()).limit(10)</code>",
                    "<code>df.limit(10).orderBy(col('salary').desc())</code>",
                    "<code>df.sort(col('salary').desc()).show(10)</code>",
                    "<code>df.filter(col('salary') > 200000).orderBy(col('salary').desc())</code>"
                ],
                "answer": 0,
                "explanation": "A: <code>orderBy().limit()</code> lets Catalyst optimize with a top-K approach — it can find the top 10 without a full sort of all 500M rows. B is wrong — limit(10) first gives you arbitrary 10 rows, then sorts them. C sorts all 500M rows to find 10 to show (no plan optimization). D only works if you can filter first, but still does a full sort of filtered results."
            },
            {
                "type": "mcq",
                "question": "You're writing a daily Parquet export that must be sorted by <code>user_id</code> within each partition file for efficient downstream reads, but global sort order doesn't matter. What should you use?",
                "options": [
                    "<code>df.orderBy('user_id')</code> — only correct option",
                    "<code>df.sortWithinPartitions('user_id')</code> — sorts locally, no shuffle",
                    "<code>df.repartition(200, 'user_id').orderBy('user_id')</code>",
                    "<code>df.sort('user_id').coalesce(200)</code>"
                ],
                "answer": 1,
                "explanation": "<code>sortWithinPartitions()</code> sorts rows within each existing partition — zero shuffle cost, zero network transfer. Perfect when readers process files independently (e.g., binary search within a Parquet file). <code>orderBy()</code> forces a global range-partition shuffle — expensive and unnecessary here."
            }
        ]
    },
    "dungeon2/m1.json": {
        "theory": [
            {"type": "text", "content": "<strong>join()</strong> in Spark combines two DataFrames based on a matching key — equivalent to SQL <code>JOIN</code>. Under the hood, Spark selects a physical join strategy: <em>Sort-Merge Join</em>, <em>Broadcast Hash Join</em>, or <em>Shuffle Hash Join</em>. Choosing the wrong strategy (or letting Spark choose poorly) is the #1 cause of out-of-memory errors and overnight job failures at MAANG scale."},
            {"type": "heading", "content": "Join strategies compared"},
            {"type": "table",
             "headers": ["Strategy", "When used", "Cost"],
             "rows": [
                 ["Broadcast Hash Join", "Small table (&lt;10 MB default, configurable)", "Zero shuffle — best perf"],
                 ["Sort-Merge Join", "Both tables large, sorted by key", "2 shuffles + sort — expensive but scalable"],
                 ["Shuffle Hash Join", "Medium unequal tables", "1 shuffle + hash table in memory"]
             ]},
            {"type": "code", "content": "from pyspark.sql.functions import broadcast\n\n# Default join (Spark picks strategy)\nresult = df_orders.join(df_customers, on='customer_id', how='inner')\n\n# Force broadcast join for small lookup table\nresult = df_orders.join(broadcast(df_regions), on='region_id', how='left')\n\n# Multi-column join key\nresult = df_a.join(df_b, on=['country', 'year'], how='inner')\n\n# Inequality join (only Sort-Merge or nested-loop)\nresult = df_a.join(df_b, df_a.date.between(df_b.start, df_b.end), how='inner')"},
            {"type": "callout", "style": "warning", "icon": "⚠️", "content": "<strong>Beware of data skew in joins.</strong> If 80% of orders belong to one customer_id (e.g., 'Unknown'), all those rows land on the same executor — causing OOM or stragglers. Fix with <em>salting</em>: add a random suffix to the skewed key, explode the small table to match, then join on the salted key."},
            {"type": "callout", "style": "tip", "icon": "💡", "content": "Always check the physical plan with <code>df.explain(mode='formatted')</code>. Look for <code>BroadcastHashJoin</code> vs <code>SortMergeJoin</code>. If Spark picked SortMergeJoin for a 5 MB lookup table, the auto-broadcast threshold may be misconfigured (<code>spark.sql.autoBroadcastJoinThreshold</code>, default 10 MB)."}
        ],
        "rune_cards": [
            {"term": "Broadcast Join", "definition": "Spark sends a full copy of the small table to every executor. Zero shuffle — fastest possible join. Triggered automatically when table size &lt; <code>spark.sql.autoBroadcastJoinThreshold</code> (default 10 MB) or manually via <code>broadcast()</code>."},
            {"term": "Sort-Merge Join", "definition": "Default for large-large joins. Both tables are shuffled by join key, sorted, then merged. Requires 2 full shuffles — the most expensive join type."},
            {"term": "Data Skew", "definition": "One key has disproportionately more rows (e.g., null or 'unknown'). Causes one task to process 1000× more data than others — OOM or straggler. Fix: salt the key or isolate the skewed value."},
            {"term": "how parameter", "definition": "Join type: <code>'inner'</code>, <code>'left'</code> (left outer), <code>'right'</code>, <code>'full'</code> (full outer), <code>'left_semi'</code>, <code>'left_anti'</code>, <code>'cross'</code>."}
        ],
        "quiz": [
            {
                "type": "mcq",
                "question": "You're joining a 2 TB orders table with a 3 MB country lookup table. What should you do?",
                "options": [
                    "Use the default join — Spark will auto-detect the broadcast opportunity",
                    "Explicitly use broadcast(df_countries) and verify with explain() that BroadcastHashJoin is used",
                    "Repartition both tables to the same number of partitions before joining",
                    "Use left_semi join to minimize data transfer"
                ],
                "answer": 1,
                "explanation": "While Spark may auto-broadcast at 3 MB (under the 10 MB default threshold), explicitly using <code>broadcast()</code> is safer — it guarantees the strategy regardless of stats availability and documents intent for reviewers. Always verify with <code>explain()</code> that you actually get a <code>BroadcastHashJoin</code> and not a <code>SortMergeJoin</code>."
            },
            {
                "type": "spot_the_bug",
                "question": "This join produces duplicate columns and downstream code crashes:",
                "code": "result = df_orders.join(df_customers, df_orders.customer_id == df_customers.customer_id)\nresult.select('customer_id')  # AnalysisException: Ambiguous references",
                "options": [
                    "You must use how='inner' explicitly — default join type is cross join",
                    "When joining on a condition (not a string column name), both customer_id columns are kept, creating ambiguity. Use on='customer_id' (string) or drop the duplicate after joining",
                    "df_orders and df_customers must have the same schema for join to work",
                    "select() after join always fails — use withColumn() instead"
                ],
                "answer": 1,
                "explanation": "When you join using a Column condition (<code>df_a.col == df_b.col</code>), Spark keeps BOTH columns in the output — resulting in two columns both named <code>customer_id</code>. Subsequent <code>select('customer_id')</code> is ambiguous and raises <code>AnalysisException</code>. Fix: use <code>join(df_b, on='customer_id')</code> (string key) which deduplicates automatically, or drop one column after joining."
            }
        ]
    },
    "dungeon2/m2.json": {
        "theory": [
            {"type": "text", "content": "<strong>Window functions</strong> compute a value for each row based on a group of related rows (a <em>window frame</em>) — without collapsing rows like <code>groupBy</code> does. In SQL this is <code>OVER (PARTITION BY ... ORDER BY ...)</code>. Use cases: rankings, running totals, moving averages, lead/lag comparisons, percentiles."},
            {"type": "heading", "content": "Window specification"},
            {"type": "code", "content": "from pyspark.sql.functions import rank, dense_rank, row_number, sum, avg, lag, lead\nfrom pyspark.sql.window import Window\n\n# Define window: partition by dept, order by salary desc\nw = Window.partitionBy('department').orderBy(col('salary').desc())\n\n# Rank employees within their department\ndf_ranked = df.withColumn('rank', rank().over(w))\ndf_ranked = df.withColumn('dense_rank', dense_rank().over(w))  # no gaps\ndf_ranked = df.withColumn('row_num', row_number().over(w))     # unique, no ties\n\n# Running total of salary within dept\nw_running = Window.partitionBy('department').orderBy('hire_date').rowsBetween(Window.unboundedPreceding, Window.currentRow)\ndf.withColumn('running_payroll', sum('salary').over(w_running))"},
            {"type": "heading", "content": "rank() vs dense_rank() vs row_number()"},
            {"type": "table",
             "headers": ["Function", "Ties handled how", "Example output (scores: 100, 100, 90)"],
             "rows": [
                 ["<code>rank()</code>", "Tied rows get same rank, next rank skips", "1, 1, 3"],
                 ["<code>dense_rank()</code>", "Tied rows get same rank, no gap after", "1, 1, 2"],
                 ["<code>row_number()</code>", "Each row gets unique number regardless of ties", "1, 2, 3 (arbitrary for ties)"]
             ]},
            {"type": "callout", "style": "warning", "icon": "⚠️", "content": "<strong>Window functions trigger a shuffle</strong> on the PARTITION BY key. A window with no PARTITION BY is equivalent to a single global partition — ALL data is sent to one executor. Never use <code>Window.orderBy()</code> without <code>partitionBy()</code> on large datasets."}
        ],
        "rune_cards": [
            {"term": "Window Spec", "definition": "Defines the frame for a window function: <code>Window.partitionBy('dept').orderBy('salary')</code>. Passed to a function via <code>.over(window_spec)</code>."},
            {"term": "PARTITION BY", "definition": "Splits data into independent groups for the window calculation. Think of it as a groupBy that doesn't collapse rows. No PARTITION BY = one global window = full shuffle to one executor."},
            {"term": "rank() vs dense_rank()", "definition": "<code>rank()</code> skips numbers after ties (1,1,3). <code>dense_rank()</code> doesn't skip (1,1,2). Use dense_rank() for award tiers; rank() for competition standings."},
            {"term": "rowsBetween / rangeBetween", "definition": "Window frame boundaries. <code>rowsBetween(Window.unboundedPreceding, Window.currentRow)</code> = running total. <code>rangeBetween(-6, 0)</code> = rolling 7-day window on an ordered integer."}
        ],
        "quiz": [
            {
                "type": "mcq",
                "question": "You need the top-2 earners per department, keeping all their columns. Which approach is correct?",
                "options": [
                    "<code>df.groupBy('department').agg(max('salary'), second_max('salary'))</code>",
                    "Define a window with <code>partitionBy('department').orderBy(desc('salary'))</code>, add <code>row_number()</code>, then filter <code>rn &lt;= 2</code>",
                    "<code>df.orderBy(desc('salary')).limit(2)</code>",
                    "Use <code>rank()</code> in a window and filter rank == 1 or rank == 2"
                ],
                "answer": 1,
                "explanation": "Window + row_number() is the correct pattern. B and D both work (D works if there are no ties — row_number is safer for exact N rows). A is wrong: there's no <code>second_max</code> function in PySpark. C gives global top-2, not per-department."
            },
            {
                "type": "spot_the_bug",
                "question": "A data engineer wrote this for a running total of orders per user. The result is unexpectedly a full total (same value for all rows). Why?",
                "code": "from pyspark.sql.window import Window\nw = Window.partitionBy('user_id')\ndf_result = df.withColumn('running_total', sum('amount').over(w))",
                "options": [
                    "sum() is not supported as a window function — use agg() instead",
                    "The window has no ORDER BY — without ordering, the frame covers all rows in the partition, so every row gets the full partition sum",
                    "Window functions require .cache() on the input DataFrame",
                    "partitionBy() and sum() conflict — use groupBy() for sum"
                ],
                "answer": 1,
                "explanation": "Without <code>orderBy()</code>, Spark uses an unbounded frame (all rows in partition) — every row gets the total partition sum. For a running total, add ordering: <code>Window.partitionBy('user_id').orderBy('order_date').rowsBetween(Window.unboundedPreceding, Window.currentRow)</code>"
            }
        ]
    },
    "dungeon2/m3.json": {
        "theory": [
            {"type": "text", "content": "<strong>unions and deduplication</strong>: <code>union()</code>/<code>unionByName()</code> stacks two DataFrames vertically (like SQL <code>UNION ALL</code>). <code>distinct()</code> removes duplicate rows. <code>dropDuplicates()</code> removes duplicates based on a subset of columns — more flexible and faster than <code>distinct()</code> for targeted dedup."},
            {"type": "heading", "content": "union() vs unionByName()"},
            {"type": "table",
             "headers": ["Method", "Column matching", "When to use"],
             "rows": [
                 ["<code>union()</code>", "By position (column order)", "Both DataFrames have identical schema in same order"],
                 ["<code>unionByName()</code>", "By column name", "Schemas have same names but different order — safer"],
                 ["<code>unionByName(allowMissingColumns=True)</code>", "By name, fills missing with null", "Merging DataFrames where one has extra columns"]
             ]},
            {"type": "code", "content": "# Stack two DataFrames\ncombined = df_jan.union(df_feb)        # positional — risky if schemas differ\ncombined = df_jan.unionByName(df_feb)  # by name — always safer\n\n# UNION ALL (default) vs UNION DISTINCT\ncombined_all = df_a.union(df_b)             # keeps duplicates (like UNION ALL)\ncombined_distinct = df_a.union(df_b).distinct() # removes duplicates\n\n# dropDuplicates on subset of columns (MAANG dedup pattern)\n# Keep most recent record per user (assuming ordered input)\ndeduped = df.dropDuplicates(['user_id', 'event_date'])"},
            {"type": "callout", "style": "warning", "icon": "⚠️", "content": "<strong>distinct() and dropDuplicates() both trigger a shuffle</strong> — they need to group identical rows from all partitions. At MAANG scale, avoid careless <code>distinct()</code> calls. Instead, use a deterministic key to deduplicate with <code>dropDuplicates(['id'])</code> or window function dedup (rank/row_number + filter)."},
            {"type": "callout", "style": "tip", "icon": "💡", "content": "Window-based dedup pattern for latest record per key:<br/><code>w = Window.partitionBy('user_id').orderBy(desc('event_ts'))<br/>df.withColumn('rn', row_number().over(w)).filter(col('rn') == 1).drop('rn')</code>"}
        ],
        "rune_cards": [
            {"term": "union() vs UNION DISTINCT", "definition": "<code>df.union()</code> = SQL <code>UNION ALL</code> — keeps all rows including duplicates. Add <code>.distinct()</code> to get <code>UNION DISTINCT</code> behavior."},
            {"term": "unionByName()", "definition": "Safer than union(): matches columns by name instead of position. Use when schemas may have different column ordering. Add <code>allowMissingColumns=True</code> for partial schema overlap."},
            {"term": "dropDuplicates()", "definition": "<code>df.dropDuplicates(['col1', 'col2'])</code> removes rows where the subset matches. More efficient than <code>distinct()</code> when you only care about certain columns. Both trigger a shuffle."},
            {"term": "Window Dedup", "definition": "Use <code>row_number().over(Window.partitionBy('id').orderBy(desc('ts')))</code> + filter for deterministic last-record-wins deduplication without risking arbitrary row selection."}
        ],
        "quiz": [
            {
                "type": "mcq",
                "question": "Two DataFrames — df_2023 has columns [user_id, amount, region] and df_2024 has columns [amount, user_id, region]. You run <code>df_2023.union(df_2024)</code>. What happens?",
                "options": [
                    "Spark matches columns by name — result is correct",
                    "Spark matches columns by position — amount from df_2024 maps to user_id column, corrupting data silently",
                    "Spark throws a schema mismatch error",
                    "Spark automatically sorts columns alphabetically before union"
                ],
                "answer": 1,
                "explanation": "<code>union()</code> matches by column position, not name. df_2024's first column (amount) maps to df_2023's first column (user_id) — silent data corruption. Always use <code>unionByName()</code> when column order might differ. This is a real production bug that has caused data incidents."
            },
            {"type": "mcq",
                "question": "You have a 5-billion row events table and need to deduplicate keeping the latest event per user_id. Which is the most efficient?",
                "options": [
                    "<code>df.distinct()</code>",
                    "<code>df.dropDuplicates(['user_id'])</code>",
                    "Window: <code>row_number().over(Window.partitionBy('user_id').orderBy(desc('event_ts')))</code> then filter <code>rn==1</code>",
                    "<code>df.groupBy('user_id').agg(max('event_ts'))</code> then rejoin"
                ],
                "answer": 2,
                "explanation": "Option B (<code>dropDuplicates(['user_id'])</code>) keeps an arbitrary row per user — not necessarily the latest. Option D requires a join (two shuffles). Option C uses one shuffle (for the window partition) and is deterministic: it always keeps the row with the latest event_ts. This is the MAANG standard dedup pattern."
            }
        ]
    },
    "dungeon2/m4.json": {
        "theory": [
            {"type": "text", "content": "<strong>UDFs (User-Defined Functions)</strong> let you apply custom Python logic to DataFrame columns. But they come with a <strong>massive performance tax</strong>: each row is serialized from JVM to Python (via Py4J), processed, then serialized back. At MAANG scale, a naive UDF can be 10-100× slower than an equivalent built-in Spark function."},
            {"type": "heading", "content": "UDF vs Pandas UDF vs Built-ins"},
            {"type": "table",
             "headers": ["Type", "Serialization", "Throughput", "When to use"],
             "rows": [
                 ["<code>spark.udf.register / udf()</code>", "Row-by-row JVM→Python (Pickle)", "~10-100× slower", "Last resort — tiny datasets or no built-in exists"],
                 ["<code>pandas_udf</code> (Arrow)", "Batch via Apache Arrow (zero-copy)", "~2-5× slower than built-ins", "Complex logic, ML inference, string processing at medium scale"],
                 ["Built-in SQL functions", "Native JVM — no serialization", "Fastest (1×)", "Always prefer if available"]
             ]},
            {"type": "code", "content": "from pyspark.sql.functions import udf, pandas_udf, col\nfrom pyspark.sql.types import StringType, DoubleType\nimport pandas as pd\n\n# Row-by-row UDF (slowest)\n@udf(returnType=StringType())\ndef classify_salary(salary):\n    if salary > 100000: return 'High'\n    elif salary > 60000: return 'Mid'\n    return 'Low'\n\ndf.withColumn('tier', classify_salary(col('salary')))\n\n# Pandas UDF (vectorized via Arrow — much faster)\n@pandas_udf(DoubleType())\ndef adjusted_salary(salary: pd.Series) -> pd.Series:\n    return salary * 1.15\n\ndf.withColumn('adj_salary', adjusted_salary(col('salary')))\n\n# BEST: use built-in when() instead of UDF\nfrom pyspark.sql.functions import when\ndf.withColumn('tier', when(col('salary')>100000,'High').when(col('salary')>60000,'Mid').otherwise('Low'))"},
            {"type": "callout", "style": "warning", "icon": "⚠️", "content": "<strong>UDFs are opaque to Catalyst.</strong> Spark cannot optimize inside a UDF — no predicate pushdown, no constant folding, no code generation. Every built-in function call is JVM-native and Catalyst-optimized. Before writing a UDF, search the <code>pyspark.sql.functions</code> documentation — there are 300+ built-in functions including most string, date, math, and array operations you'll need."},
            {"type": "callout", "style": "info", "icon": "ℹ️", "content": "For ML inference at scale, use <strong>pandas_udf</strong> with Arrow serialization, or better yet <strong>Spark MLlib</strong> transformers or <strong>Spark + MLflow</strong> model serving — these avoid Python serialization entirely for structured prediction."}
        ],
        "rune_cards": [
            {"term": "UDF (User-Defined Function)", "definition": "Custom Python function registered to run on DataFrame columns. Each row serialized JVM→Python→JVM via Pickle. Black box to Catalyst — no optimization possible. ~10-100× slower than built-ins."},
            {"term": "Pandas UDF (Vectorized UDF)", "definition": "Processes entire batches of data as Pandas Series via Apache Arrow (near zero-copy). Annotated with <code>@pandas_udf</code>. 5-20× faster than row-by-row UDFs. Still slower than native Spark functions."},
            {"term": "Apache Arrow", "definition": "In-memory columnar data format. Bridge between JVM (Spark) and Python (Pandas). Eliminates Pickle serialization overhead in Pandas UDFs — data stays in memory-mapped columnar format."},
            {"term": "when() / otherwise()", "definition": "PySpark's native conditional expression (<code>CASE WHEN</code> in SQL). Fully Catalyst-optimized. Replace most classification UDFs: <code>when(cond1, v1).when(cond2, v2).otherwise(vN)</code>."}
        ],
        "quiz": [
            {
                "type": "mcq",
                "question": "A pipeline classifies 1B rows using a Python UDF that checks if a string contains a keyword. A code review suggests replacing it. What's the best native alternative?",
                "options": [
                    "Register the same logic as a pandas_udf instead",
                    "Use <code>col('text').contains('keyword')</code> or <code>col('text').rlike('pattern')</code> — built-in string functions",
                    "Use <code>df.filter()</code> to pre-filter rows before applying the UDF",
                    "Add more executors to compensate for UDF overhead"
                ],
                "answer": 1,
                "explanation": "PySpark has native built-in string functions: <code>contains()</code>, <code>rlike()</code> (regex), <code>like()</code>, <code>startswith()</code>, <code>instr()</code>. These run in JVM with Catalyst optimization — zero Python serialization. At 1B rows, this can mean hours of difference in run time."
            },
            {
                "type": "spot_the_bug",
                "question": "A UDF is registered and called but returns all nulls. Find the bug:",
                "code": "@udf\ndef double_salary(salary):\n    return salary * 2\n\ndf.withColumn('doubled', double_salary(col('salary')))",
                "options": [
                    "UDFs cannot perform arithmetic — use withColumn instead",
                    "@udf decorator requires an explicit return type: @udf(returnType=DoubleType()) — without it, Spark treats return type as StringType and can't cast, producing nulls",
                    "col('salary') must be cast to Python float before passing to a UDF",
                    "UDFs must be registered with spark.udf.register() before use"
                ],
                "answer": 1,
                "explanation": "Without specifying <code>returnType</code>, PySpark defaults the UDF return type to <code>StringType</code>. When the result (a number like 190000) can't be coerced to a string cleanly in context, Spark may return nulls or the wrong type. Always declare: <code>@udf(returnType=DoubleType())</code>"
            }
        ]
    },
    "dungeon2/m5.json": {
        "theory": [
            {"type": "text", "content": "<strong>Caching and persistence</strong>: <code>df.cache()</code> materializes a DataFrame in executor memory so subsequent actions reuse the computed result without re-executing the full lineage. Critical for iterative algorithms (ML model training) or when a DataFrame is used in multiple downstream operations."},
            {"type": "heading", "content": "Storage levels"},
            {"type": "table",
             "headers": ["Level", "What's stored", "When to use"],
             "rows": [
                 ["<code>MEMORY_ONLY</code> (default)", "Deserialized JVM objects in RAM", "Enough memory, need fastest access"],
                 ["<code>MEMORY_AND_DISK</code>", "RAM first, spill to disk", "Large DataFrames — safe default"],
                 ["<code>MEMORY_ONLY_SER</code>", "Serialized (smaller) in RAM", "Tight memory, tolerate CPU cost of deser"],
                 ["<code>DISK_ONLY</code>", "Always on disk", "Too large for memory, recompute expensive"]
             ]},
            {"type": "code", "content": "from pyspark import StorageLevel\n\n# cache() = MEMORY_AND_DISK in modern Spark datasets\ndf_expensive = df.join(df_lookup, 'id').filter(col('active') == True)\ndf_expensive.cache()      # lazy — nothing happens yet\ndf_expensive.count()      # triggers materialization\n\n# Explicit storage level\ndf_expensive.persist(StorageLevel.MEMORY_AND_DISK)\n\n# Always unpersist when done to free executor memory\ndf_expensive.unpersist()\n\n# Check if cached\ndf_expensive.is_cached   # True / False\ndf_expensive.storageLevel # StorageLevel object"},
            {"type": "callout", "style": "warning", "icon": "⚠️", "content": "<strong>cache() is lazy.</strong> Calling <code>df.cache()</code> alone does nothing — the DataFrame is only materialized when the first action (<code>count()</code>, <code>show()</code>, <code>write</code>) is executed. Always follow <code>cache()</code> with an action to pre-warm the cache."},
            {"type": "callout", "style": "warning", "icon": "⚠️", "content": "<strong>Always call unpersist().</strong> Cached DataFrames hold executor memory until the SparkContext is stopped or the application is killed — unless you explicitly call <code>df.unpersist()</code>. Forgetting this is a common cause of executor OOM in long-running Spark applications."},
            {"type": "callout", "style": "tip", "icon": "💡", "content": "At MAANG scale, prefer <strong>checkpointing</strong> over caching for very long lineage DAGs (100+ transformations). <code>df.checkpoint()</code> truncates the lineage and saves to HDFS/S3 — tolerant to executor failures unlike in-memory cache."}
        ],
        "rune_cards": [
            {"term": "cache()", "definition": "Marks a DataFrame for reuse across multiple actions. Default storage level: <code>MEMORY_AND_DISK</code>. Lazy — materialized on first action. Must call <code>unpersist()</code> to free memory."},
            {"term": "persist(StorageLevel)", "definition": "Like cache() but with explicit storage level control. Options: MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, MEMORY_ONLY_SER, etc. Import from <code>pyspark import StorageLevel</code>."},
            {"term": "DAG Lineage", "definition": "Spark's record of all transformations from source to current DataFrame. On failure, Spark recomputes from last checkpoint. Long lineages are slow to retry — cache or checkpoint at key steps."},
            {"term": "Checkpoint", "definition": "<code>df.checkpoint()</code> materializes the DataFrame to disk (HDFS/S3) AND truncates the lineage DAG. Unlike cache(), survives executor failures. Required for iterative ML with many cycles."}
        ],
        "quiz": [
            {
                "type": "mcq",
                "question": "A DataFrame is used in 5 different downstream transformations. When is the optimal time to call cache()?",
                "options": [
                    "Before the DataFrame is defined → cache() + lazy eval means it's computed once when first used",
                    "After defining the expensive DataFrame (joins, filters) but before the first action that uses it in multiple branches",
                    "After all 5 downstream operations complete — Spark retroactively caches it",
                    "cache() is only useful for RDDs, not DataFrames"
                ],
                "answer": 1,
                "explanation": "Call <code>cache()</code> after defining the expensive DataFrame but before any action. Then run one action (like <code>count()</code>) to materialize it. All subsequent actions reuse the cached result. Caching before definition is meaningless (nothing to cache). Option C is wrong — Spark doesn't retroactively cache."
            },
            {
                "type": "spot_the_bug",
                "question": "An engineer 'caches' a DataFrame but the job runs just as slowly the second time. What's wrong?",
                "code": "df_processed = df.join(df_lookup, 'id').filter(col('active')==True)\ndf_processed.cache()\n\n# ... later in the job ...\ndf_processed.groupBy('dept').count().show()\ndf_processed.groupBy('region').sum('revenue').show()",
                "options": [
                    "cache() only applies to the first action — the second action recomputes from scratch",
                    "cache() is lazy — it was declared but no action was run immediately after it to materialize the cache. The first show() pays full cost AND caches simultaneously, so the second show() benefits but the first doesn't",
                    "You can't cache DataFrames that contain joins",
                    "cache() doesn't work with groupBy operations"
                ],
                "answer": 1,
                "explanation": "The cache is <em>lazily materialized</em> on the first action — so the first <code>groupBy().count().show()</code> still pays the full join + filter cost (while also writing to cache). Only the second action benefits. Fix: add <code>df_processed.count()</code> immediately after <code>cache()</code> to pre-warm the cache before the first downstream action."
            }
        ]
    }
}


def main():
    updated = []
    errors = []
    for rel_path, extra_fields in MISSIONS.items():
        fpath = BASE / rel_path
        if not fpath.exists():
            errors.append(f"NOT FOUND: {fpath}")
            continue
        data = json.loads(fpath.read_text())
        if "concept" not in data:
            data["concept"] = {}
        data["concept"].update(extra_fields)
        fpath.write_text(json.dumps(data, indent=2, ensure_ascii=False))
        updated.append(str(rel_path))

    print(f"Updated {len(updated)} files:")
    for f in updated:
        print(f"  ✅ {f}")
    if errors:
        print("Errors:")
        for e in errors:
            print(f"  ❌ {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

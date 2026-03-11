"""
Add theory_scroll field to dungeon meta.json files.
"""
import json, pathlib

BASE = pathlib.Path(__file__).parent.parent / "data" / "missions"

d1_meta = json.loads((BASE / "dungeon1/meta.json").read_text())
d1_meta["theory_scroll"] = [
    {
        "type": "text",
        "content": "<strong>Dungeon I covers the five core DataFrame operations</strong> every PySpark engineer uses daily: <code>select()</code>, <code>filter()</code>, <code>withColumn()</code>, <code>groupBy().agg()</code>, and <code>orderBy()</code>. Mastering these — and understanding what happens under the hood — is the foundation for everything else in Spark."
    },
    {
        "type": "table",
        "headers": ["Operation", "SQL equivalent", "Key consideration"],
        "rows": [
            ["<code>select()</code>", "SELECT col1, col2", "Column pruning — read only what you need from Parquet"],
            ["<code>filter()</code>", "WHERE clause", "Use &amp; | ~ not and/or; predicates get pushed to file reader"],
            ["<code>withColumn()</code>", "computed column AS", "Avoid chaining 20+ calls — use select() instead"],
            ["<code>groupBy().agg()</code>", "GROUP BY + agg fn", "2-phase aggregate: partial (local) then shuffle then final"],
            ["<code>orderBy()</code>", "ORDER BY", "Global sort = range-partition shuffle. Pair with limit(N)"]
        ]
    },
    {
        "type": "callout",
        "style": "tip",
        "icon": "\U0001f4a1",
        "content": "Run <code>df.explain(mode='formatted')</code> on any query to see what Spark actually plans to do. Learning to read query plans is the single most impactful skill for performance debugging at MAANG."
    }
]
(BASE / "dungeon1/meta.json").write_text(json.dumps(d1_meta, indent=2, ensure_ascii=False))
print("Updated dungeon1/meta.json")

d2_meta = json.loads((BASE / "dungeon2/meta.json").read_text())
d2_meta["theory_scroll"] = [
    {
        "type": "text",
        "content": "<strong>Dungeon II covers the operations that move or reshape data across the cluster</strong> — joins, window functions, unions, UDFs, and caching. These are where most Spark performance issues live at MAANG scale, and where junior engineers make the most expensive mistakes."
    },
    {
        "type": "table",
        "headers": ["Operation", "Cost", "Key gotcha"],
        "rows": [
            ["<code>join()</code>", "Sort-Merge: 2 shuffles. Broadcast: 0 shuffles", "Skew on join key use broadcast() or salting"],
            ["Window functions", "1 shuffle on PARTITION BY key", "No PARTITION BY = full data to 1 executor = OOM"],
            ["<code>union()</code> / <code>distinct()</code>", "distinct() = full shuffle", "unionByName() vs union() by position — silent corruption risk"],
            ["UDFs", "Row-by-row JVM-to-Python serialization", "Always prefer built-in functions; use pandas_udf if must"],
            ["<code>cache()</code> / <code>persist()</code>", "Memory pressure", "Lazy — must run action to materialize. Always unpersist() when done"]
        ]
    },
    {
        "type": "callout",
        "style": "warning",
        "icon": "\u26a0\ufe0f",
        "content": "The #1 production incident cause at MAANG data teams: <strong>data skew on join keys</strong> (especially null/unknown values). Always check key distribution with <code>df.groupBy(key).count().orderBy('count', ascending=False).show(10)</code> before joining."
    }
]
(BASE / "dungeon2/meta.json").write_text(json.dumps(d2_meta, indent=2, ensure_ascii=False))
print("Updated dungeon2/meta.json")
print("Done.")

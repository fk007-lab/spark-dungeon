import contextlib
import io
import traceback
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

DATASETS_DIR = Path(__file__).parent.parent / "data" / "datasets"

_SAFE_BUILTINS = {
    "__builtins__": {
        "print": print,
        "len": len,
        "range": range,
        "list": list,
        "dict": dict,
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "enumerate": enumerate,
        "zip": zip,
        "sorted": sorted,
        "reversed": reversed,
        "map": map,
        "filter": filter,
        "isinstance": isinstance,
        "type": type,
        "abs": abs,
        "round": round,
        "min": min,
        "max": max,
        "sum": sum,
        "None": None,
        "True": True,
        "False": False,
    }
}


def load_dataset(spark: SparkSession, filename: str) -> DataFrame:
    path = str(DATASETS_DIR / filename)
    if filename.endswith(".parquet"):
        return spark.read.parquet(path)
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


def run_code(
    code: str,
    spark: SparkSession,
    mission: dict,
) -> dict[str, Any]:
    """
    Execute user code in a restricted namespace.
    Returns:
        {
            "success": bool,
            "result_df": DataFrame | None,
            "explain_plan": str,
            "error": str | None,
        }
    """
    # Load primary dataset (and optional secondary)
    dataset = mission.get("dataset")
    dataset2 = mission.get("dataset2")

    try:
        df = load_dataset(spark, dataset) if dataset else None
        df2 = load_dataset(spark, dataset2) if dataset2 else None
    except Exception as e:
        return {"success": False, "result_df": None, "explain_plan": "", "error": f"Dataset load error: {e}"}

    namespace = {
        **_SAFE_BUILTINS,
        "spark": spark,
        "F": F,
        "Window": Window,
        "df": df,
    }
    if df2 is not None:
        namespace["df2"] = df2

    try:
        exec(compile(code, "<mission>", "exec"), namespace)  # noqa: S102
    except SyntaxError as e:
        return {"success": False, "result_df": None, "explain_plan": "", "error": f"SyntaxError: {e}"}
    except Exception as e:
        return {"success": False, "result_df": None, "explain_plan": "", "error": traceback.format_exc(limit=5)}

    result = namespace.get("result")
    if result is None:
        return {
            "success": False,
            "result_df": None,
            "explain_plan": "",
            "error": "No variable named `result` found. Make sure your code assigns to `result`.",
        }

    if not isinstance(result, DataFrame):
        return {
            "success": False,
            "result_df": None,
            "explain_plan": "",
            "error": f"`result` must be a Spark DataFrame, got {type(result).__name__}.",
        }

    # Extract explain plan
    explain_buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(explain_buf):
            result.explain(mode="formatted")
        explain_plan = explain_buf.getvalue()
    except Exception:
        explain_plan = "(explain plan unavailable)"

    return {
        "success": True,
        "result_df": result,
        "explain_plan": explain_plan,
        "error": None,
    }

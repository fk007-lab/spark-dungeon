from pyspark.sql import DataFrame
from typing import Any


def _row_to_dict(row) -> dict:
    return {k: (float(v) if hasattr(v, "__float__") and not isinstance(v, (int, bool, str)) else v)
            for k, v in row.asDict().items()}


def _sort_key(d: dict):
    return sorted((str(k), str(v)) for k, v in d.items())


def validate(actual_df: DataFrame, expected: list[dict]) -> dict[str, Any]:
    """
    Order-insensitive comparison of actual DataFrame rows vs expected list of dicts.
    Column names must match; row values are compared after sorting both sides.
    Returns: {passed: bool, hint: str}
    """
    try:
        actual_rows = [_row_to_dict(r) for r in actual_df.collect()]
    except Exception as e:
        return {"passed": False, "hint": f"Could not collect result DataFrame: {e}"}

    # Normalise expected: convert all numeric strings to numbers where possible
    def _normalise(d: dict) -> dict:
        out = {}
        for k, v in d.items():
            if isinstance(v, str):
                for cast in (int, float):
                    try:
                        v = cast(v)
                        break
                    except (ValueError, TypeError):
                        pass
            out[k] = v
        return out

    expected_norm = [_normalise(r) for r in expected]
    actual_norm = [_normalise(r) for r in actual_rows]

    # Column check
    if actual_rows:
        actual_cols = set(actual_rows[0].keys())
        expected_cols = set(expected[0].keys()) if expected else set()
        missing = expected_cols - actual_cols
        extra = actual_cols - expected_cols
        if missing:
            return {"passed": False, "hint": f"Missing columns: {sorted(missing)}"}
        if extra:
            return {"passed": False, "hint": f"Unexpected extra columns: {sorted(extra)}. Select only the required columns."}

    # Row count check
    if len(actual_norm) != len(expected_norm):
        return {
            "passed": False,
            "hint": f"Row count mismatch: got {len(actual_norm)}, expected {len(expected_norm)}.",
        }

    # Sort both sides and compare
    actual_sorted = sorted(actual_norm, key=_sort_key)
    expected_sorted = sorted(expected_norm, key=_sort_key)

    for i, (a, e) in enumerate(zip(actual_sorted, expected_sorted)):
        if a != e:
            # Find first differing key
            for key in e:
                if a.get(key) != e.get(key):
                    return {
                        "passed": False,
                        "hint": f"Row {i+1}: column `{key}` — expected `{e[key]}`, got `{a.get(key)}`.",
                    }
            return {"passed": False, "hint": f"Row {i+1} differs from expected."}

    return {"passed": True, "hint": ""}

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "progress" / "player.db"


def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db() -> None:
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS progress (
                mission_id TEXT PRIMARY KEY,
                status     TEXT    NOT NULL DEFAULT 'locked',
                attempts   INTEGER NOT NULL DEFAULT 0,
                solved_at  TEXT
            )
        """)


def seed_missions(all_mission_ids: list[str], first_unlocked: str) -> None:
    """Insert rows for every mission if they don't exist yet."""
    with _conn() as con:
        for mid in all_mission_ids:
            status = "unlocked" if mid == first_unlocked else "locked"
            con.execute(
                "INSERT OR IGNORE INTO progress (mission_id, status) VALUES (?, ?)",
                (mid, status),
            )


def get_status(mission_id: str) -> str:
    with _conn() as con:
        row = con.execute(
            "SELECT status FROM progress WHERE mission_id = ?", (mission_id,)
        ).fetchone()
    return row["status"] if row else "locked"


def increment_attempts(mission_id: str) -> None:
    with _conn() as con:
        con.execute(
            "UPDATE progress SET attempts = attempts + 1 WHERE mission_id = ?",
            (mission_id,),
        )


def mark_solved(mission_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute(
            "UPDATE progress SET status = 'solved', solved_at = ? WHERE mission_id = ?",
            (now, mission_id),
        )


def unlock(mission_id: str) -> None:
    with _conn() as con:
        con.execute(
            "UPDATE progress SET status = 'unlocked' WHERE mission_id = ? AND status = 'locked'",
            (mission_id,),
        )


def get_all() -> dict[str, dict]:
    with _conn() as con:
        rows = con.execute("SELECT * FROM progress").fetchall()
    return {row["mission_id"]: dict(row) for row in rows}


def reset_all(all_mission_ids: list[str], first_unlocked: str) -> None:
    with _conn() as con:
        con.execute("DELETE FROM progress")
    seed_missions(all_mission_ids, first_unlocked)

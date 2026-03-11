import json
import os
from pathlib import Path
from typing import Any

_missions: dict[str, dict] = {}
_dungeons: dict[str, dict] = {}

MISSIONS_DIR = Path(__file__).parent.parent / "data" / "missions"


def _load_all() -> None:
    if _missions:
        return
    for dungeon_dir in sorted(MISSIONS_DIR.iterdir()):
        if not dungeon_dir.is_dir():
            continue
        dungeon_id = dungeon_dir.name
        meta_path = dungeon_dir / "meta.json"
        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)
            meta["id"] = dungeon_id
            meta["missions"] = []
            _dungeons[dungeon_id] = meta

        for mission_file in sorted(dungeon_dir.glob("m*.json")):
            with open(mission_file) as f:
                mission = json.load(f)
            mission_id = mission["id"]
            _missions[mission_id] = mission
            if dungeon_id in _dungeons:
                _dungeons[dungeon_id]["missions"].append(mission_id)


def get_all_dungeons() -> list[dict]:
    _load_all()
    return list(_dungeons.values())


def get_dungeon(dungeon_id: str) -> dict | None:
    _load_all()
    return _dungeons.get(dungeon_id)


def get_mission(mission_id: str) -> dict | None:
    _load_all()
    return _missions.get(mission_id)


def get_missions_for_dungeon(dungeon_id: str) -> list[dict]:
    _load_all()
    dungeon = _dungeons.get(dungeon_id)
    if not dungeon:
        return []
    return [_missions[mid] for mid in dungeon["missions"] if mid in _missions]

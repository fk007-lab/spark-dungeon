import json
from pathlib import Path
from typing import Optional

CODEX_DIR = Path(__file__).parent.parent / "data" / "codex"

_index: Optional[dict] = None
_chapters: dict[str, dict] = {}


def get_codex_index() -> dict:
    global _index
    if _index is None:
        idx_path = CODEX_DIR / "index.json"
        _index = json.loads(idx_path.read_text()) if idx_path.exists() else {"volumes": []}
    return _index


def get_chapter(chapter_id: str) -> Optional[dict]:
    if chapter_id not in _chapters:
        path = CODEX_DIR / f"{chapter_id}.json"
        if path.exists():
            _chapters[chapter_id] = json.loads(path.read_text())
        else:
            return None
    return _chapters[chapter_id]

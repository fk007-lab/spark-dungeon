from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from backend.spark_session import get_spark, stop_spark
from backend.mission_loader import (
    get_all_dungeons,
    get_dungeon,
    get_mission,
    get_missions_for_dungeon,
)
from backend import progression
from backend.runner import run_code
from backend.validator import validate
from backend.codex_loader import get_codex_index, get_chapter

BASE_DIR = Path(__file__).parent.parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    spark = get_spark()
    progression.init_db()
    # Seed progress for any missions not yet in DB
    dungeons = get_all_dungeons()
    all_ids = [mid for d in dungeons for mid in d.get("missions", [])]
    first_id = all_ids[0] if all_ids else ""
    progression.seed_missions(all_ids, first_id)
    yield
    # Shutdown
    stop_spark()


app = FastAPI(title="Spark Dungeon", lifespan=lifespan)

app.mount(
    "/static",
    StaticFiles(directory=str(BASE_DIR / "frontend" / "static")),
    name="static",
)

templates = Jinja2Templates(directory=str(BASE_DIR / "frontend" / "templates"))


# ── Routes ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dungeon_list(request: Request):
    dungeons = get_all_dungeons()
    all_progress = progression.get_all()

    for d in dungeons:
        mission_ids = d.get("missions", [])
        solved = sum(1 for mid in mission_ids if all_progress.get(mid, {}).get("status") == "solved")
        total = len(mission_ids)
        d["solved"] = solved
        d["total"] = total
        d["pct"] = int(solved / total * 100) if total else 0
        first_status = all_progress.get(mission_ids[0], {}).get("status", "locked") if mission_ids else "locked"
        d["unlocked"] = first_status in ("unlocked", "solved")

    return templates.TemplateResponse("dungeons.html", {"request": request, "dungeons": dungeons})


@app.get("/dungeon/{dungeon_id}", response_class=HTMLResponse)
async def dungeon_detail(request: Request, dungeon_id: str):
    dungeon = get_dungeon(dungeon_id)
    if not dungeon:
        raise HTTPException(status_code=404, detail="Dungeon not found")

    missions = get_missions_for_dungeon(dungeon_id)
    all_progress = progression.get_all()

    for m in missions:
        m["status"] = all_progress.get(m["id"], {}).get("status", "locked")

    return templates.TemplateResponse(
        "missions.html",
        {"request": request, "dungeon": dungeon, "missions": missions},
    )


@app.get("/mission/{mission_id}", response_class=HTMLResponse)
async def mission_detail(request: Request, mission_id: str):
    mission = get_mission(mission_id)
    if not mission:
        raise HTTPException(status_code=404, detail="Mission not found")

    status = progression.get_status(mission_id)
    if status == "locked":
        raise HTTPException(status_code=403, detail="Mission is locked")

    dungeon = get_dungeon(mission["dungeon_id"])
    missions_in_dungeon = get_missions_for_dungeon(mission["dungeon_id"])
    all_progress = progression.get_all()

    # Determine next mission
    ids = [m["id"] for m in missions_in_dungeon]
    current_idx = ids.index(mission_id) if mission_id in ids else -1
    next_mission_id = ids[current_idx + 1] if current_idx >= 0 and current_idx + 1 < len(ids) else None
    next_status = all_progress.get(next_mission_id, {}).get("status", "locked") if next_mission_id else None

    return templates.TemplateResponse(
        "mission.html",
        {
            "request": request,
            "mission": mission,
            "dungeon": dungeon,
            "status": status,
            "next_mission_id": next_mission_id,
            "next_status": next_status,
        },
    )


# ── Run endpoint ─────────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    code: str


@app.post("/mission/{mission_id}/run")
async def run_mission(mission_id: str, body: RunRequest):
    mission = get_mission(mission_id)
    if not mission:
        raise HTTPException(status_code=404, detail="Mission not found")

    status = progression.get_status(mission_id)
    if status == "locked":
        raise HTTPException(status_code=403, detail="Mission is locked")

    spark = get_spark()
    progression.increment_attempts(mission_id)

    run_result = run_code(body.code, spark, mission)

    if not run_result["success"]:
        return JSONResponse({
            "passed": False,
            "error": run_result["error"],
            "result_preview": [],
            "explain_plan": "",
            "hint": run_result["error"],
            "next_mission_id": None,
        })

    result_df = run_result["result_df"]
    validation = validate(result_df, mission.get("expected", []))

    # Build result preview (first 10 rows)
    try:
        preview_rows = [r.asDict() for r in result_df.limit(10).collect()]
        # Convert non-serialisable types to strings
        preview_rows = [{k: (str(v) if not isinstance(v, (int, float, bool, str, type(None))) else v)
                         for k, v in row.items()} for row in preview_rows]
    except Exception:
        preview_rows = []

    next_mission_id = None
    if validation["passed"] and status != "solved":
        progression.mark_solved(mission_id)
        # Unlock next mission within dungeon
        dungeon_missions = get_missions_for_dungeon(mission["dungeon_id"])
        ids = [m["id"] for m in dungeon_missions]
        idx = ids.index(mission_id) if mission_id in ids else -1
        if idx >= 0 and idx + 1 < len(ids):
            next_mission_id = ids[idx + 1]
            progression.unlock(next_mission_id)
        else:
            # Last mission in dungeon — unlock first mission of next dungeon
            all_dungeons = get_all_dungeons()
            dungeon_ids = [d["id"] for d in all_dungeons]
            d_idx = dungeon_ids.index(mission["dungeon_id"]) if mission["dungeon_id"] in dungeon_ids else -1
            if d_idx >= 0 and d_idx + 1 < len(dungeon_ids):
                next_dungeon = get_dungeon(dungeon_ids[d_idx + 1])
                if next_dungeon and next_dungeon.get("missions"):
                    first_of_next = next_dungeon["missions"][0]
                    progression.unlock(first_of_next)

    hints = mission.get("hints", [])
    attempts = progression.get_all().get(mission_id, {}).get("attempts", 1)
    hint_to_show = hints[min(attempts - 1, len(hints) - 1)] if hints and not validation["passed"] else ""

    return JSONResponse({
        "passed": validation["passed"],
        "error": None,
        "result_preview": preview_rows,
        "explain_plan": run_result["explain_plan"],
        "hint": hint_to_show if not validation["passed"] else "",
        "validation_hint": validation["hint"],
        "next_mission_id": next_mission_id,
    })


@app.get("/api/progress")
async def api_progress():
    return progression.get_all()


@app.post("/api/reset")
async def api_reset():
    dungeons = get_all_dungeons()
    all_ids = [mid for d in dungeons for mid in d.get("missions", [])]
    first_id = all_ids[0] if all_ids else ""
    progression.reset_all(all_ids, first_id)
    return {"reset": True}


# ── Codex routes ─────────────────────────────────────────────────────────────

@app.get("/codex", response_class=HTMLResponse)
async def codex_index(request: Request):
    index = get_codex_index()
    return templates.TemplateResponse(
        "codex_index.html",
        {"request": request, "volumes": index.get("volumes", [])},
    )


@app.get("/codex/{chapter_id}", response_class=HTMLResponse)
async def codex_chapter(request: Request, chapter_id: str):
    chapter = get_chapter(chapter_id)
    if not chapter:
        raise HTTPException(status_code=404, detail="Chapter not found")
    return templates.TemplateResponse(
        "codex_chapter.html",
        {"request": request, "chapter": chapter},
    )

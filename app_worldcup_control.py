"""Serve the traffic dashboard and push its live state through FastAPI WebSockets."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, WebSocketException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import redis
import redis.asyncio as async_redis
from watchfiles import awatch

from worldcup.analytics import cell_geojson, forecast_next_counts, hotel_view, latest_rows, next_alert, recommend_shuttle
from worldcup.config import ROOT, Settings
from worldcup.mapbox import CDN, STYLE, public_token
from worldcup.redis_store import VIEW_CHANNEL, processed_key, recent_aggregates, recent_hotels
from worldcup.reference import cities_by_name, load_reference


def read_rows(client):
    return recent_aggregates(client, "cell"), recent_aggregates(client, "venue"), recent_hotels(client)


def build_view(client, reference: dict, city: str, previous_alerts: dict, settings: Settings) -> dict:
    return city_view(read_rows(client), reference, city, previous_alerts, settings)


def city_view(rows, reference, city, previous_alerts, settings):
    cells, venues, hotels = ([row for row in group if row["city"] == city] for group in rows)
    current_venues = latest_rows(venues, ("city", "venue", "source"))
    alerts = []
    recommendations = []
    forecasts = {}
    state = dict(previous_alerts or {})
    city_reference = cities_by_name(reference)[city]
    seen = set()
    for row in current_venues:
        source = row["source"]
        identity = f"{city}:{row['venue']}:{source}"
        seen.add(identity)
        pressure = row.get("pressure")
        was_active = bool(state.get(identity))
        active = next_alert(was_active, pressure, settings.alert_high, settings.alert_low)
        state[identity] = active
        forecasts[source] = forecast_next_counts(item for item in venues if item["source"] == source and item["venue"] == row["venue"])
        if active:
            alerts.append(row)
            route = recommend_shuttle(city_reference, pressure, settings.alert_low if was_active else settings.alert_high)
            if route and not recommendations:
                recommendations.append(route)
    for identity in list(state):
        if identity.startswith(f"{city}:") and identity not in seen:
            state.pop(identity)
    latest_update = max((row["updated_at"] for row in [*cells, *venues, *hotels]), default=None)
    return {
        "geojson": cell_geojson(cells),
        "venues": current_venues,
        "hotels": hotel_view(hotels),
        "alerts": alerts,
        "recommendations": recommendations,
        "forecasts": forecasts,
        "alert_state": state,
        "latest_update": latest_update,
        "center": city_reference["center"],
        "site": city_reference.get("site"),
    }


class DashboardFeed:
    """One Redis subscription and one materialized snapshot per city, shared by browsers."""

    def __init__(self, settings, reference, stage_file, *, benchmark_ws=False):
        self.settings = settings
        self.reference = reference
        self.stage_file = stage_file
        self.benchmark_ws = benchmark_ws
        self.client = redis.Redis.from_url(
            settings.redis_url, decode_responses=True, socket_connect_timeout=2, socket_timeout=2
        )
        self.cities = list(cities_by_name(reference))
        self.views = {}
        self.alert_states = {}
        self.stage = None
        self.passages = {}
        self.listeners = set()
        self.dirty = asyncio.Event()

    def notify(self):
        for listener in self.listeners:
            listener.set()

    def load_views(self):
        benchmark_processed = None
        try:
            if self.benchmark_ws:
                # Read before the aggregates so this is a lower bound on visible commits.
                benchmark_processed = int(self.client.get(processed_key(self.settings.stream_id)) or 0)
            rows = read_rows(self.client)
            available = True
        except redis.RedisError:
            if self.views:
                return {city: {**frame, "available": False} for city, frame in self.views.items()}
            rows = ([], [], [])
            available = False

        frames = {}
        for city in self.cities:
            view = city_view(rows, self.reference, city, self.alert_states.get(city), self.settings)
            self.alert_states[city] = view.pop("alert_state")
            # The browser displays stale/fresh. An advancing age alone isn't a visible change.
            for hotel in view["hotels"]:
                hotel.pop("age_seconds", None)
            frames[city] = {"type": "view", "city": city, "available": available, "view": view}
            if self.benchmark_ws and available:
                frames[city]["benchmark_processed_total"] = benchmark_processed
        return frames

    async def update_views(self):
        while True:
            self.dirty.clear()
            frames = await asyncio.to_thread(self.load_views)
            if frames != self.views:
                self.views = frames
                self.notify()
            # Windows expire and hotel snapshots go stale even when all producers are idle.
            try:
                await asyncio.wait_for(self.dirty.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

    async def listen_to_redis(self):
        async with async_redis.Redis.from_url(
            self.settings.redis_url, decode_responses=True, socket_connect_timeout=2
        ) as client:
            while True:
                try:
                    async with client.pubsub() as events:
                        await events.subscribe(VIEW_CHANNEL)
                        async for _ in events.listen():
                            # A subscription acknowledgement also reconciles writes missed during an outage.
                            self.dirty.set()
                except redis.RedisError:
                    self.dirty.set()
                    await asyncio.sleep(2)

    def load_presentation_file(self, path, attribute):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            value = {} if attribute == "passages" else None
        except (OSError, ValueError):
            return
        if value != getattr(self, attribute):
            setattr(self, attribute, value)
            self.notify()

    async def watch_stage(self):
        files = {self.stage_file: "stage", self.stage_file.with_name("passages.json"): "passages"}
        for path, attribute in files.items():
            self.load_presentation_file(path, attribute)
        async for changes in awatch(self.stage_file.parent, recursive=False, debounce=50,
                                    watch_filter=lambda change, path: Path(path) in files,
                                    yield_on_timeout=True, rust_timeout=30_000):
            for path in {Path(name) for _, name in changes} or files:
                self.load_presentation_file(path, files[path])


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    stage_path = os.getenv("WORLDCUP_PRESENTATION_STAGE")
    stage_file = Path(stage_path).resolve() if stage_path else None
    reference = load_reference(settings.reference_path)
    offline = os.getenv("WORLDCUP_OFFLINE_MAP") == "1"
    benchmark_ws = os.getenv("WORLDCUP_BENCHMARK_WS") == "1"
    map_config = {"token": "" if offline else public_token(settings.mapbox_access_token),
                  "cdn": CDN, "style": STYLE, "offline": offline}

    @asynccontextmanager
    async def lifespan(app):
        feed = DashboardFeed(settings, reference, stage_file, benchmark_ws=benchmark_ws)
        app.state.feed = feed
        tasks = [asyncio.create_task(feed.listen_to_redis()), asyncio.create_task(feed.update_views())]
        if stage_file:
            tasks.append(asyncio.create_task(feed.watch_stage()))
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            feed.client.close()

    app = FastAPI(lifespan=lifespan, openapi_url=None)
    app.mount("/assets", StaticFiles(directory=ROOT / "assets"), name="assets")
    app.mount("/vendor", StaticFiles(directory=ROOT / "pitch" / "dist" / "vendor"), name="vendor")
    if stage_file:
        app.mount("/pitch", StaticFiles(directory=ROOT / "pitch" / "dist", html=True), name="pitch")

    @app.get("/")
    async def dashboard():
        return FileResponse(ROOT / "assets" / "dashboard.html", headers={"Cache-Control": "no-cache"})

    @app.websocket("/ws")
    async def live(socket: WebSocket):
        origin = socket.headers.get("origin")
        if origin and urlsplit(origin).netloc.lower() != socket.headers.get("host", "").lower():
            await socket.close(code=1008)
            return
        feed = socket.app.state.feed
        city = feed.cities[0]
        subscription = 0
        changed = asyncio.Event()
        await socket.accept()
        await socket.send_json({"type": "config", "cities": feed.cities, "city": city,
                                "presentation": bool(stage_file), "map": map_config})

        async def select_city():
            nonlocal city, subscription
            while True:
                packet = await socket.receive()
                if packet["type"] == "websocket.disconnect":
                    return
                try:
                    message = json.loads(packet.get("text", ""))
                except (ValueError, TypeError):
                    message = None
                if not isinstance(message, dict) or message.get("type") != "subscribe" or message.get("city") not in feed.cities:
                    raise WebSocketException(code=1008, reason="Unknown city or subscription")
                city = message["city"]
                subscription += 1
                changed.set()

        async def send_updates():
            last_view = last_stage = object()
            last_subscription = -1
            while True:
                await changed.wait()
                changed.clear()
                frame = feed.views.get(city)
                if frame and stage_file and city in feed.passages:
                    frame = {**frame, "view": {**frame["view"], "passages": feed.passages[city]}}
                if frame is not None and (frame != last_view or subscription != last_subscription):
                    last_subscription = subscription
                    await socket.send_json(frame)
                    last_view = frame
                stage = feed.stage
                if stage_file and stage != last_stage:
                    await socket.send_json({"type": "stage", "stage": stage})
                    last_stage = stage

        feed.listeners.add(changed)
        changed.set()
        tasks = [asyncio.create_task(select_city()), asyncio.create_task(send_updates())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except WebSocketDisconnect:
            pass
        finally:
            feed.listeners.discard(changed)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    return app


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(create_app(), host="127.0.0.1", port=int(os.getenv("DASH_PORT", "8050")))

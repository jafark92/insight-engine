"""Simplified insights service.

Phase 1: only provide a recent alerts listing with optional basic filters.
Phase 2 (later): add per-AUV telemetry time-series (temperature, depth_m, location, velocity, etc.).

This keeps implementation minimal for frontend to start consuming alert streams.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Iterable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

ALERT_TYPES = {"environmental", "zone_violation", "dead_auv"}
SUMMARY_MODES_ALLOWED = {"timeseries", "stats"}
TIMESERIES_ALLOWED_FIELDS = {"temperature_c", "depth_m", "velocity_knots", "location"}


@dataclass
class InsightParams:
    auv_id: Optional[str] = None
    type: Optional[str] = None
    limit: int = 20
    summary: bool = False  # backward compatibility; if true implies timeseries
    summary_modes: Optional[List[str]] = None  # e.g. ["timeseries","stats"]
    window_minutes: int = 20  # window for applicable summaries
    timeseries_limit: int = 30  # max telemetry points returned
    timeseries_fields: Optional[List[str]] = None  # subset of allowed fields
    since: Optional[datetime] = None  # alerts after this timestamp
    cursor: Optional[str] = None  # pagination cursor "<iso>|<id>"


def _parse_point_wkt(wkt: Optional[str]) -> Optional[Tuple[float, float]]:
    if not wkt:
        return None
    # Expected format: POINT(lon lat)
    wkt = wkt.strip()
    if not wkt.upper().startswith("POINT(") or not wkt.endswith(")"):
        return None
    inner = wkt[wkt.find("(") + 1 : -1]
    parts = inner.replace(",", " ").split()
    if len(parts) != 2:
        return None
    try:
        lon = float(parts[0])
        lat = float(parts[1])
        return lon, lat
    except ValueError:
        return None


async def fetch_insights(session_factory: async_sessionmaker[AsyncSession], insight_params: InsightParams) -> Dict[str, Any]:
    """Return recent alerts plus optional summaries.

    Summaries selected via summary_modes (list). Supported modes:
      - timeseries (requires auv_id)
      - stats (lightweight aggregate counts)
    """
    insight_params.limit = max(1, min(insight_params.limit, 100))
    insight_params.timeseries_limit = max(10, min(insight_params.timeseries_limit, 2000))
    # Derive active modes
    modes: List[str] = []
    if insight_params.summary and (not insight_params.summary_modes):
        modes.append("timeseries") # Use timeseries if mode is not provided
    if insight_params.summary_modes:
        for m in insight_params.summary_modes:
            if m not in modes:  # preserve order
                modes.append(m)
    modes = [m for m in modes if m in SUMMARY_MODES_ALLOWED]

    # Filter & paginate Alerts
    filters: List[str] = []
    params: Dict[str, Any] = {"limit": insight_params.limit}
    if insight_params.auv_id:
        filters.append("auv_id = :auv_id")
        params["auv_id"] = insight_params.auv_id
    if insight_params.type:
        filters.append("type = :type")
        params["type"] = insight_params.type
    if insight_params.since:
        filters.append("started_at > :since")
        params["since"] = insight_params.since
    if insight_params.cursor:
        try:
            c_time, c_id = insight_params.cursor.split("|", 1)
            c_dt = datetime.fromisoformat(c_time)
            c_id_int = int(c_id)
            filters.append("(started_at < :cursor_started_at OR (started_at = :cursor_started_at AND id < :cursor_id))")
            params["cursor_started_at"] = c_dt
            params["cursor_id"] = c_id_int
        except Exception:
            pass
    where_clause = ("WHERE " + " AND ".join(filters)) if filters else ""    
    sql = text(
        f"""
        SELECT auv_id, type, severity, status, message, started_at
        FROM alerts
        {where_clause}
        ORDER BY started_at DESC, id DESC
        LIMIT :limit
        """
    )
    alerts: List[Dict[str, Any]] = []
    next_cursor: Optional[str] = None
    async with session_factory() as session:
        res = await session.execute(sql, params)
        rows = res.fetchall()
        for r in rows:
            alerts.append(
                {
                    "auv_id": r.auv_id,
                    "type": r.type,
                    "severity": r.severity,
                    "status": r.status,
                    "message": r.message,
                    "started_at": r.started_at.isoformat() if r.started_at else None,
                }
            )
        if rows and len(rows) == insight_params.limit:
            last = rows[-1]
            if last.started_at is not None:
                next_cursor = f"{last.started_at.isoformat()}|{last.id}"

    out: Dict[str, Any] = {"alerts": alerts}
    if next_cursor:
        out["pagination"] = {"next_cursor": next_cursor}
    if not modes: 
        return out

    # If Summary Mode is passed
    summaries: Dict[str, Any] = {}
    window_start = datetime.now(timezone.utc) - timedelta(minutes=insight_params.window_minutes)

    if "timeseries" in modes:
        if not insight_params.auv_id:
            summaries["timeseries_error"] = "timeseries summary requires auv_id"
        else:
            if insight_params.timeseries_fields:
                requested_fields: Iterable[str] = [f for f in insight_params.timeseries_fields if f in TIMESERIES_ALLOWED_FIELDS]
            else:
                requested_fields = list(TIMESERIES_ALLOWED_FIELDS)
            ts_sql = text(
                """
                SELECT timestamp, temperature_c, depth_m, velocity_knots, location_wkt
                FROM telemetry
                WHERE auv_id = :auv_id AND timestamp >= :window_start
                ORDER BY timestamp ASC
                LIMIT :ts_limit
                """
            )
            ts_params = {
                "auv_id": insight_params.auv_id,
                "window_start": window_start,
                "ts_limit": insight_params.timeseries_limit,
            }
            points: List[Dict[str, Any]] = []
            async with session_factory() as session:
                res_ts = await session.execute(ts_sql, ts_params)
                for row in res_ts.fetchall():
                    ll = _parse_point_wkt(row.location_wkt)
                    loc = {"lon": ll[0], "lat": ll[1]} if ll else None
                    pt: Dict[str, Any] = {"timestamp": row.timestamp.isoformat() if row.timestamp else None}
                    if "temperature_c" in requested_fields:
                        pt["temperature_c"] = row.temperature_c
                    if "depth_m" in requested_fields:
                        pt["depth_m"] = row.depth_m
                    if "velocity_knots" in requested_fields:
                        pt["velocity_knots"] = row.velocity_knots
                    if "location" in requested_fields:
                        pt["location"] = loc
                    points.append(pt)
            summaries["timeseries"] = {
                "auv_id": insight_params.auv_id,
                "window_minutes": insight_params.window_minutes,
                "fields": list(requested_fields),
                "points": points,
                "count": len(points),
            }

    if "stats" in modes:
        # Lightweight aggregate over alerts (respecting filters for auv_id / type)
        stats_filters: List[str] = []
        stats_params: Dict[str, Any] = {"window_start": window_start}
        if insight_params.auv_id:
            stats_filters.append("auv_id = :auv_id")
            stats_params["auv_id"] = insight_params.auv_id
        if insight_params.type:
            stats_filters.append("type = :type")
            stats_params["type"] = insight_params.type
        stats_where = ("WHERE " + " AND ".join(stats_filters)) if stats_filters else ""
        stats_sql = text(
            f"""
            SELECT
              COUNT(*) AS total_alerts,
              MAX(started_at) AS latest_alert,
              SUM(CASE WHEN started_at >= :window_start THEN 1 ELSE 0 END) AS alerts_in_window
            FROM alerts
            {stats_where}
            """
        )
        by_type_sql = text(
            f"""
            SELECT type, COUNT(*) AS c
            FROM alerts
            {stats_where}
            GROUP BY type
            """
        )
        async with session_factory() as session:
            res_stats = await session.execute(stats_sql, stats_params)
            row_s = res_stats.first()
            res_bt = await session.execute(by_type_sql, stats_params)
            alerts_by_type = {r.type: r.c for r in res_bt.fetchall()}
        summaries["stats"] = {
            "window_minutes": insight_params.window_minutes,
            "total_alerts": (row_s.total_alerts if row_s else 0),
            "alerts_in_window": (row_s.alerts_in_window if row_s else 0),
            "latest_alert_timestamp": row_s.latest_alert.isoformat() if row_s and row_s.latest_alert else None,
            "alerts_by_type": alerts_by_type,
        }

    out["summaries"] = summaries
    return out

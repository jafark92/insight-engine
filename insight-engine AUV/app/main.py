from contextlib import asynccontextmanager
import asyncio
import json
from typing import List
from datetime import datetime, timezone
import aiohttp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketState

from app.config.thresholds import ENVIRONMENTAL_THRESHOLDS
from app.services.environmental_monitor import EnvironmentalMonitor

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize services
    """Start the telemetry monitoring when the application starts"""
    asyncio.create_task(monitor_telemetry())
    yield
    # Shutdown: Cleanup resources
    print("Shutting down Service...")

app = FastAPI(
    title="DeepSeaGuard Insight Engine",
    description="Real-time monitoring and alert system for AUV operations",
    version="0.1.0",
    lifespan=lifespan
)

# Initialize environmental monitor
env_monitor = EnvironmentalMonitor(ENVIRONMENTAL_THRESHOLDS)

class ConnectionManager:
    """Manages WebSocket connections for broadcasting messages to clients"""
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self.active_connections.append(websocket)
            print(f"Client connected. Total connections: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
                print(f"Client disconnected. Remaining connections: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        async with self._lock:
            disconnected = []
            for connection in self.active_connections:
                try:
                    if connection.client_state == WebSocketState.CONNECTED:
                        await connection.send_json(message)
                except Exception as e:
                    print(f"Error broadcasting to client: {e}")
                    disconnected.append(connection)
            
            # Clean up disconnected clients
            for conn in disconnected:
                await self.disconnect(conn)

manager = ConnectionManager()

@app.get("/")
async def root():
    return JSONResponse(
        content={"message": "Welcome to DeepSeaGuard Insight Engine"},
        status_code=200
    )

@app.websocket("/ws/alert")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for clients to receive environmental alerts
    """
    await manager.connect(websocket)
    try:
        while True:
            try:
                data = await websocket.receive_json()
                # Echo back received data with timestamp
                await websocket.send_json({
                    "type": "echo",
                    "data": data,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid JSON format"
                })
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket error: {e}")
        if websocket.client_state == WebSocketState.CONNECTED:
            await websocket.close(code=1011)  # Internal error
        await manager.disconnect(websocket)

async def monitor_telemetry():
    """
    Background task to monitor External telemetry Data and generate alerts
    """
    reconnect_delay = 5  # Reconnect delay
    
    async def check_for_alert(telemetry: dict):
        """Process telemetry data and check against environmental thresholds"""
        if alert := env_monitor.check_thresholds(telemetry):
            await manager.broadcast({
                "type": "alert",
                "data": alert,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            print(f"Alert generated: {alert}")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    'ws://localhost:8001/ws/telemetry',
                    heartbeat=30,  # Enable heartbeat every 30 seconds
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as ws:
                    print("Connected to mock telemetry websocket")
                    
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            print(f"Received message: {msg.data}")
                            try:
                                telemetry = json.loads(msg.data)
                                await check_for_alert(telemetry)
                            except json.JSONDecodeError as e:
                                print(f"Invalid JSON received: {e}")
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print(f"WebSocket error: {ws.exception()}")
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            print("WebSocket connection closed")
                            break

        except aiohttp.ClientError as e:
            print(f"Connection error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

        print("Reconnecting to telemetry WebSocket...")
        await asyncio.sleep(reconnect_delay)
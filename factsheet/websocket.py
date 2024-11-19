from fastapi import WebSocket
from typing import Dict, Set
import logging

logger = logging.getLogger(__name__)

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}

    async def connect(self, job_id: str, websocket: WebSocket):
        """Store a new WebSocket connection."""
        if job_id not in self.active_connections:
            self.active_connections[job_id] = set()
        self.active_connections[job_id].add(websocket)
        logger.info(f"WebSocket connected for job {job_id}")

    async def disconnect(self, websocket: WebSocket, job_id: str):
        """Disconnect a WebSocket connection."""
        try:
            if job_id in self.active_connections:
                if websocket in self.active_connections[job_id]:
                    self.active_connections[job_id].remove(websocket)

                    try:
                        await websocket.close(code=1000, reason="Normal closure")
                    except Exception as e:
                        logger.error(f"Error closing WebSocket for job {job_id}: {e}")

                if not self.active_connections[job_id]:
                    del self.active_connections[job_id]

            logger.info(f"WebSocket disconnected for job {job_id}")
        except Exception as e:
            logger.error(f"Error during WebSocket disconnect for job {job_id}: {e}")

    async def send_update(self, job_id: str, data: dict):
        """Send an update to connected WebSocket clients."""
        if job_id in self.active_connections:
            for websocket in self.active_connections[job_id]:
                try:
                    await websocket.send_json(data)
                except Exception as e:
                    logger.error(f"Error sending update to WebSocket for job {job_id}: {e}")
                    await self.disconnect(websocket, job_id)


# Global connection manager instance
manager = ConnectionManager()

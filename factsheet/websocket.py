from fastapi import WebSocket
from typing import Dict, Set, Optional
import logging
import json
from datetime import datetime
import jwt

logger = logging.getLogger(__name__)

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self.authenticated_jobs: set[str] = set()
        
    async def connect(self, job_id: str, websocket: WebSocket):
        """Store a new WebSocket connection."""
        # Clean up any existing connection first
        if job_id in self.active_connections:
            await self.disconnect(websocket, job_id)
        if job_id not in self.active_connections:
            self.active_connections[job_id] = set()
        self.active_connections[job_id].add(websocket)
        logger.info(f"WebSocket connected for job {job_id}")
        
    async def authenticate(self, websocket: WebSocket, token: str, contract_id: str, job_id: str) -> bool:
        """Authenticate a WebSocket connection."""
        try:
            # Parse and validate the auth message
            if not token:
                await websocket.send_json({
                    "type": "error",
                    "message": "No authentication token provided"
                })
                return False

            # Verify the token and get user_id
            user_id = await self.verify_token(token)
            if not user_id:
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid authentication token"
                })
                return False

            # Verify contract access
            if not await self.verify_contract_access(contract_id, user_id):
                await websocket.send_json({
                    "type": "error",
                    "message": "No access to contract"
                })
                return False

            # Add to authenticated connections
            if job_id not in self.active_connections:
                self.active_connections[job_id] = set()
            self.active_connections[job_id].add(websocket)
            self.authenticated_jobs.add(job_id)
            
            # Send auth success message
            await websocket.send_json({"type": "auth_success"})
            logger.info(f"WebSocket authenticated for job {job_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error during WebSocket authentication for job {job_id}: {e}")
            try:
                await websocket.send_json({
                    "type": "error",
                    "message": "Authentication error"
                })
            except Exception as close_error:
                logger.error(f"Error sending auth error for job {job_id}: {close_error}")
            return False

    async def disconnect(self, websocket: WebSocket, job_id: str):
        """Disconnect a WebSocket connection."""
        try:
            if job_id in self.active_connections:
                if websocket in self.active_connections[job_id]:
                    self.active_connections[job_id].remove(websocket)
                    self.authenticated_jobs.discard(job_id)
                    
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
        """Send an update to a connected WebSocket client."""
        if job_id not in self.authenticated_jobs:
            logger.error(f"Attempted to send update to unauthenticated job {job_id}")
            return
            
        if job_id in self.active_connections:
            for websocket in self.active_connections[job_id]:
                try:
                    # Add timestamp to all messages
                    data["timestamp"] = datetime.utcnow().isoformat()
                    await websocket.send_json(data)
                    logger.debug(f"Sent update for job {job_id}: {data['type']}")
                except Exception as e:
                    logger.error(f"Error sending update for job {job_id}: {e}")
                    await self.disconnect(websocket, job_id)
                
    def is_authenticated(self, job_id: str) -> bool:
        """Check if a job is authenticated."""
        return job_id in self.authenticated_jobs
        
    def get_connection(self, job_id: str) -> Set[WebSocket] | None:
        """Get the WebSocket connection for a job if it exists."""
        return self.active_connections.get(job_id)
    
    async def close_all(self):
        """Close all active connections."""
        for job_id in list(self.active_connections.keys()):
            for websocket in self.active_connections[job_id]:
                await self.disconnect(websocket, job_id)

    async def broadcast(self, data: dict):
        """Send an update to all connected clients."""
        disconnected = []
        for job_id, websockets in self.active_connections.items():
            if not self.is_authenticated(job_id):
                continue
            for websocket in websockets:
                try:
                    await websocket.send_json(data)
                except Exception as e:
                    logger.error(f"Error broadcasting to job {job_id}: {e}")
                    disconnected.append((websocket, job_id))
                
        # Clean up disconnected clients
        for websocket, job_id in disconnected:
            await self.disconnect(websocket, job_id)

    async def verify_token(self, token: str) -> Optional[str]:
        """Verify JWT token and return user_id if valid."""
        try:
            decoded = jwt.decode(token, options={"verify_signature": False})
            return decoded.get("sub")
        except Exception as e:
            logger.error(f"Error verifying token: {e}")
            return None

    async def verify_contract_access(self, contract_id: str, user_id: str) -> bool:
        """Verify user has access to contract using Supabase RLS."""
        try:
            from shared.dependencies import get_supabase_client
            supabase = get_supabase_client()
            result = (
                supabase.table("contracts")
                .select("id")
                .eq("id", contract_id)
                .eq("owner_id", user_id)
                .execute()
            )
            return bool(result.data)
        except Exception as e:
            logger.error(f"Error verifying contract access: {e}")
            return False

# Global connection manager instance
manager = ConnectionManager()

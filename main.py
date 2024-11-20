from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

from document_converter.route import router as document_converter_router
from factsheet.route import router as factsheet_router
from factsheet.service import FactsheetService
from factsheet.websocket import manager

app = FastAPI()
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources on application shutdown."""
    logger.info("Shutting down application...")
    
    # Close Redis connection
    if FactsheetService._redis_client:
        await FactsheetService._redis_client.close()
        FactsheetService._redis_client = None
        logger.info("Closed Redis connection")
    
    # Close all WebSocket connections
    for job_id in list(manager.active_connections.keys()):
        for websocket in list(manager.active_connections[job_id]):
            await manager.disconnect(websocket, job_id)
    logger.info("Closed all WebSocket connections")

app.include_router(document_converter_router, prefix="", tags=["document-converter"])
app.include_router(factsheet_router, prefix="", tags=["factsheet"])

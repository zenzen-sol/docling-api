import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv(".env")
celery_app = Celery(
    "celery_app",
    broker=os.environ.get("REDIS_HOST", "redis://localhost:6379/0"),
    backend=os.environ.get("REDIS_HOST", "redis://localhost:6379/0"),
    include=["tasks"],
)

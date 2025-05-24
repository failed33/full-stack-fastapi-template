from fastapi import APIRouter

from app.api.routes import files, items, login, private, uploads, users, utils
from app.core.config import settings
from app.sse_stream.router import router as sse_router

api_router = APIRouter()
api_router.include_router(login.router)
api_router.include_router(users.router)
api_router.include_router(utils.router)
api_router.include_router(items.router)
api_router.include_router(uploads.router)
api_router.include_router(files.router)
api_router.include_router(sse_router, prefix="/sse", tags=["SSE"])


if settings.ENVIRONMENT == "local":
    api_router.include_router(private.router)

from fastapi import APIRouter
from .routes import health, files, conversions, converters, jobs, docs, settings, default_formats

router = APIRouter()

# Include all route modules
router.include_router(health.router)
router.include_router(files.router)
router.include_router(conversions.router)
router.include_router(converters.router)
router.include_router(jobs.router)
router.include_router(settings.router)
router.include_router(default_formats.router)
router.include_router(docs.router)

"""File upload and download API endpoints using MinIO."""

from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel

from app.services.file_storage import FileStorageService
from app.minio_client import get_public_url
from app.schemas.common import MessageResponse

router = APIRouter(prefix="/files", tags=["files"])


class FileUploadResponse(BaseModel):
    file_id: str
    object_name: str
    url: str
    filename: str
    content_type: str
    size: int
    category: str


class TeamLogoResponse(BaseModel):
    object_name: str
    url: str
    team_name: str
    content_type: str


class TeamLogosListResponse(BaseModel):
    logos: list
    count: int


class FilesListResponse(BaseModel):
    files: list
    count: int


# ============ Team logos endpoints (specific routes first) ============

@router.get("/teams/logos", response_model=TeamLogosListResponse)
async def list_team_logos():
    """List all team logos."""
    logos = await FileStorageService.list_team_logos()
    return {"logos": logos, "count": len(logos)}


@router.post("/teams/{team_name}/logo", response_model=TeamLogoResponse)
async def upload_team_logo(
    team_name: str,
    file: UploadFile = File(...),
):
    """Upload team logo (overwrites existing)."""
    content = await file.read()

    result = await FileStorageService.upload_team_logo(
        file_data=content,
        team_name=team_name,
        content_type=file.content_type or "image/webp",
    )

    return TeamLogoResponse(**result)


@router.get("/teams/{team_name}/logo")
async def get_team_logo(team_name: str):
    """Get team logo by team name."""
    result = await FileStorageService.get_team_logo(team_name)

    if not result:
        raise HTTPException(status_code=404, detail="Team logo not found")

    content, metadata = result

    return Response(
        content=content,
        media_type=metadata.get("content_type", "image/webp"),
        headers={
            "Content-Disposition": f'inline; filename="{metadata.get("filename", "logo.webp")}"',
            "Cache-Control": "public, max-age=86400",  # 24 hours cache
        },
    )


# ============ General file endpoints ============

@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    category: str = Query(default="uploads"),
    news_id: str | None = Query(default=None, description="News article ID for news images"),
    language: str | None = Query(default=None, description="Language (RU, KZ) for documents"),
):
    """Upload a file to MinIO storage with optional metadata."""
    content = await file.read()

    metadata = {}
    if news_id:
        metadata["news-id"] = news_id
    if language:
        metadata["language"] = language.upper()

    result = await FileStorageService.upload_file(
        file_data=content,
        filename=file.filename or "unknown",
        content_type=file.content_type or "application/octet-stream",
        category=category,
        metadata=metadata,
    )

    return FileUploadResponse(**result)


@router.get("/list", response_model=FilesListResponse)
async def list_files(
    category: str | None = None,
    limit: int = Query(default=100, le=1000),
):
    """List files with optional category filtering."""
    files = await FileStorageService.list_files(category=category, limit=limit)
    return {"files": files, "count": len(files)}


@router.get("/view/{category}/{file_path:path}")
async def view_file(category: str, file_path: str):
    """View a file in browser (inline). Use for PDF preview."""
    object_name = f"{category}/{file_path}"
    result = await FileStorageService.get_file(object_name)

    if not result:
        raise HTTPException(status_code=404, detail="File not found")

    content, metadata = result
    filename = metadata.get("filename", "file")

    return Response(
        content=content,
        media_type=metadata.get("content_type", "application/pdf"),
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            "Cache-Control": "public, max-age=86400",  # 24 hours cache
        },
    )


@router.get("/download/{category}/{file_path:path}")
async def download_file(category: str, file_path: str):
    """Download a file from MinIO storage."""
    object_name = f"{category}/{file_path}"
    result = await FileStorageService.get_file(object_name)

    if not result:
        raise HTTPException(status_code=404, detail="File not found")

    content, metadata = result

    return Response(
        content=content,
        media_type=metadata.get("content_type", "application/octet-stream"),
        headers={
            "Content-Disposition": f'attachment; filename="{metadata.get("filename", "file")}"',
            "Cache-Control": "public, max-age=31536000",  # 1 year cache
        },
    )


@router.delete("/{category}/{file_path:path}", response_model=MessageResponse)
async def delete_file(category: str, file_path: str):
    """Delete a file from MinIO storage."""
    object_name = f"{category}/{file_path}"
    deleted = await FileStorageService.delete_file(object_name)

    if not deleted:
        raise HTTPException(status_code=404, detail="File not found")

    return {"message": "File deleted successfully"}

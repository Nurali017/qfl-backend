from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile

from app.api.admin.deps import require_roles
from app.models import AdminUser
from app.services.file_storage import FileStorageService

router = APIRouter(prefix="/files", tags=["admin-files"])


@router.get("/list")
async def list_files(
    category: str | None = None,
    limit: int = Query(default=100, le=1000),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    files = await FileStorageService.list_files(category=category, limit=limit)
    return {"files": files, "count": len(files)}


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    category: str = Query(default="uploads"),
    news_id: str | None = Query(default=None),
    language: str | None = Query(default=None),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
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
    return result


@router.delete("/{category}/{file_path:path}")
async def delete_file(
    category: str,
    file_path: str,
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    object_name = f"{category}/{file_path}"
    deleted = await FileStorageService.delete_file(object_name)
    if not deleted:
        raise HTTPException(status_code=404, detail="File not found")
    return {"message": "File deleted"}


@router.post("/teams/{team_name}/logo")
async def upload_team_logo(
    team_name: str,
    file: UploadFile = File(...),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    content = await file.read()
    return await FileStorageService.upload_team_logo(
        file_data=content,
        team_name=team_name,
        content_type=file.content_type or "image/webp",
    )


@router.post("/countries/{country_code}/flag")
async def upload_country_flag(
    country_code: str,
    file: UploadFile = File(...),
    _admin: AdminUser = Depends(require_roles("superadmin", "editor")),
):
    content = await file.read()
    return await FileStorageService.upload_country_flag(
        file_data=content,
        country_code=country_code,
        content_type=file.content_type or "image/webp",
    )

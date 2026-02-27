# app/api/schemas/images_list.py


from datetime import datetime
from typing import Optional, Literal, List
from pydantic import BaseModel

from app.domain.images import StorageKind


class ImageListItem(BaseModel):
    uuid: str
    name: Optional[str] = None
    last_updated: datetime

    storage: StorageKind
    content_type: Optional[str] = None
    size_bytes: Optional[int] = None

    width: Optional[int] = None
    height: Optional[int] = None
    format: Optional[str] = None
    mode: Optional[str] = None

    original_url: str

class ImageListResponse(BaseModel):
    items: List[ImageListItem]
    limit: int
    offset: int
    total: int
    has_more: bool


# Практическое правило “как делать”
#
# API вход: Pydantic RequestModel
#
# API выход: Pydantic ResponseModel
#
# Service: принимает простые типы или domain-объекты, возвращает domain/dataclass
#
# Repo: принимает/возвращает domain-объекты
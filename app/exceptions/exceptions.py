# app/exceptions/exceptions.py


from __future__ import annotations

from fastapi import HTTPException, status


# ---------- STORAGE / IMAGE SERVICE ----------

InvalidStorageException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="storage must be one of: fs, mem, s3"
)

UploadFailedException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="Upload failed"
)

IngestFailedException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="Ingest failed"
)

UuidAlreadyExistsException = HTTPException(
    status_code=status.HTTP_409_CONFLICT,
    detail="UUID already exists"
)

MetadataNotFoundException = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail="Metadata not found"
)

OriginalImageNotFoundException = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail="Original image not found"
)


# ---------- TILES ----------

ManifestNotFoundException = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail="Tile manifest not found"
)

TileNotFoundException = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail="Tile not found"
)

TileReadException = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="Tile read failed"
)

TileSizeInvalidException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="tiles_tile_size must be 256 or 512"
)

TileFormatInvalidException = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="tiles_fmt must be webp or png"
)
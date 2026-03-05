from fastapi import Request
from fastapi.responses import JSONResponse

from app.exceptions.repo_errors import StorageError, StorageNotFoundError, StorageLocationError, StorageIOError, \
    StorageLimitError, MetadataError, MetadataConflictError, MetadataDBError, MetadataDataError
from app.exceptions.usecase_errors import UseCaseError, UseCaseConflictError, UseCaseValidationError, \
    UseCaseNotFoundError


def register_exception_handlers(app):

    @app.exception_handler(StorageError)
    async def storage_error_handler(request: Request, exc: StorageError):

        if isinstance(exc, StorageNotFoundError):
            status_code = 404
        elif isinstance(exc, StorageLimitError):
            status_code = 413
        elif isinstance(exc, (StorageLocationError, StorageIOError)):
            status_code = 500
        else:
            status_code = 500

        return JSONResponse(status_code=status_code, content={"detail": str(exc)})

    @app.exception_handler(MetadataError)
    async def metadata_error_handler(request: Request, exc: MetadataError):
        if isinstance(exc, MetadataConflictError):
            status_code = 409
        elif isinstance(exc, (MetadataDBError, MetadataDataError)):
            status_code = 500
        else:
            status_code = 500
        return JSONResponse(status_code=status_code, content={"detail": str(exc)})


    @app.exception_handler(UseCaseError)
    async def usecase_error_handler(request: Request, exc: UseCaseError):
        if isinstance(exc, UseCaseValidationError):
            status_code = 400
        elif isinstance(exc, UseCaseNotFoundError):
            status_code = 404
        elif isinstance(exc, UseCaseConflictError):
            status_code = 409
        else:
            status_code = 500
        return JSONResponse(status_code=status_code, content={"detail": str(exc)})
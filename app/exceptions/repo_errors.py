# app/exceptions/repo_errors.py


# ================= STORAGE ===============
class StorageError(Exception):
    """Базовая ошибка работы с хранилищем."""


class StorageLocationError(StorageError):
    """Некорректные данные location для выбранного типа хранилища."""


class StorageNotFoundError(StorageError):
    """Объект не найден в хранилище."""


class StorageIOError(StorageError):
    """Ошибка ввода-вывода при работе с хранилищем."""

class StorageLimitError(StorageError):
    """Превышен лимит хранилища."""



# ================= METADATA ===============
class MetadataError(Exception):
    """Базовая ошибка репозитория метаданных."""


class MetadataConflictError(MetadataError):
    """Конфликт метаданных (например, нарушение уникальности uuid)."""


class MetadataDBError(MetadataError):
    """Ошибка базы данных при работе с метаданными."""


class MetadataDataError(MetadataError):
    """Некорректные/битые данные метаданных в базе."""


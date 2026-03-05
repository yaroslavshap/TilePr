

class UseCaseError(Exception):
    """Базовая ошибка бизнес-логики (use-case)."""


class UseCaseConflictError(UseCaseError):
    """Конфликт (например, UUID уже существует)."""


class UseCaseValidationError(UseCaseError):
    """Ошибка валидации входных данных (например, файл не является изображением)."""


class UseCaseNotFoundError(UseCaseError):
    """Запрошенный объект не найден."""
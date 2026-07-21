__all__ = (
    "BackgroundProfile",
    "BackgroundContext",
    "BiosphereProfile",
    "ConversionError",
    "BrightwayInventory",
    "DATA_DIR",
    "ExcelSerializationError",
    "FormatDetectionError",
    "FormatProfile",
    "InventoryFormat",
    "InventoryContext",
    "InventoryPipeline",
    "InventoryValidationError",
    "Issue",
    "MigrationError",
    "MigrationUnavailableError",
    "OperationError",
    "SimaProInventory",
    "SimaProSerializationError",
    "SerializationError",
    "TechnosphereProfile",
    "ValidationReport",
)
__version__ = "1.0.0a1"

from importlib import import_module
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"

_EXPORTS = {
    "BrightwayInventory": (".brightway", "BrightwayInventory"),
    "SimaProInventory": (".simapro", "SimaProInventory"),
    "InventoryPipeline": (".core.pipeline", "InventoryPipeline"),
    "BackgroundContext": (".core", "BackgroundContext"),
    "BiosphereProfile": (".core", "BiosphereProfile"),
    "FormatProfile": (".core", "FormatProfile"),
    "InventoryContext": (".core", "InventoryContext"),
    "TechnosphereProfile": (".core", "TechnosphereProfile"),
    "BackgroundProfile": (".models", "BackgroundProfile"),
    "InventoryFormat": (".models", "InventoryFormat"),
    "Issue": (".models", "Issue"),
    "ValidationReport": (".models", "ValidationReport"),
    "ConversionError": (".exceptions", "ConversionError"),
    "ExcelSerializationError": (".exceptions", "ExcelSerializationError"),
    "FormatDetectionError": (".exceptions", "FormatDetectionError"),
    "InventoryValidationError": (".exceptions", "InventoryValidationError"),
    "MigrationError": (".exceptions", "MigrationError"),
    "MigrationUnavailableError": (".exceptions", "MigrationUnavailableError"),
    "OperationError": (".exceptions", "OperationError"),
    "SerializationError": (".exceptions", "SerializationError"),
    "SimaProSerializationError": (".exceptions", "SimaProSerializationError"),
}


def __getattr__(name: str):
    """Load public facades only when they are requested.

    In-memory migration must not import file-format adapters and ``bw2io``.
    """

    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name, __name__), attribute)
    globals()[name] = value
    return value

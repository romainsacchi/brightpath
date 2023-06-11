__all__ = ("Converter", "DATA_DIR")
__version__ = (0, 0, 1)

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"

from .converter import Converter
__all__ = ("BrightwayConverter", "SimaproConverter", "DATA_DIR")
__version__ = (0, 0, 2)

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"

from .bwconverter import BrightwayConverter
from .simaproconverter import SimaproConverter

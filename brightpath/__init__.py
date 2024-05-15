__all__ = ("BrigthwayConverter", "SimaproConverter", "DATA_DIR")
__version__ = (0, 0, 1)

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"

from .bwconverter import BrigthwayConverter
from .simaproconverter import SimaproConverter
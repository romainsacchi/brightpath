__all__ = ("BrightwayConverter", "SimaproConverter", "DATA_DIR")
__version__ = (0, 0, 3)

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"

from .bwconverter import BrightwayConverter  # noqa: E402
from .simaproconverter import SimaproConverter  # noqa: E402

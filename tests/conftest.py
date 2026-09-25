from datetime import datetime

import pytest


@pytest.fixture
def fixed_simapro_clock(monkeypatch):
    """Keep export timestamps constant when comparing repeated renders."""

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 1, 1, tzinfo=tz)

    monkeypatch.setattr("brightpath.formats.simapro_csv.datetime.datetime", FixedDatetime)

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session", autouse=True)
def sample_data():
    """Make sure data/ exists (it is regenerated deterministically if missing)."""
    if not (ROOT / "data" / "churn.csv").exists():
        subprocess.run([sys.executable, str(ROOT / "scripts" / "make_data.py")], check=True)


@pytest.fixture(scope="session")
def spark():
    from common.spark import get_spark

    session = get_spark("pytest")
    yield session
    session.stop()

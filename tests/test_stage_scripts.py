"""Run every example script end to end in a fresh interpreter."""
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = sorted(p for d in ROOT.glob("0[1-6]-*") for p in d.glob("[0-9][0-9]_*.py"))


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_script_runs(script):
    env = dict(os.environ, PYTHONUNBUFFERED="1")
    result = subprocess.run([sys.executable, str(script)], cwd=ROOT, env=env,
                            capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stderr[-2000:]

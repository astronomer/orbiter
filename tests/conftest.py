from pathlib import Path

import orbiter
import pytest


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def project_version() -> str:
    return orbiter.__version__

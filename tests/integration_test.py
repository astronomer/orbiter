import pytest

from orbiter.__main__ import run
from tests.conftest import manual_tests


# noinspection PyUnreachableCode
@pytest.mark.skip("Relies on orbiter-community-translations, not yet published")
@manual_tests
def test_integration():
    run("just docker-build-binary", shell=True, capture_output=True, text=True)

    output = run("just docker-run-binary", shell=True, capture_output=True, text=True)
    assert "Available Origins" in output.stdout
    assert (
        "Adding local .pyz files ['/data/orbiter_community_translations.pyz'] to sys.path"
        in output.stdout
    )
    assert "Finding files with extension=['.xml'] in /data/workflow" in output.stdout

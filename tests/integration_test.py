from orbiter.__main__ import run
from tests.conftest import manual_tests


@manual_tests
def test_integration():
    run("just docker-build-binary", shell=True, capture_output=True, text=True)

    output = run("just docker-run-binary", shell=True, capture_output=True, text=True)
    assert "Available Origins" in output.stdout
    assert "Adding local .pyz files ['/data/orbiter_translations.pyz'] to sys.path" in output.stdout
    assert "Translating [File 0]=/data/workflow/demo.xml" in output.stdout
    assert "Writing /data/output/dags" in output.stdout

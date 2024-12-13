#!/usr/bin/env just --justfile
set dotenv-load := true
DOCS_DIR := "docs"
SRC_DIR := "orbiter"
EXTRAS := "dev"
VERSION := `echo $(python3 -c 'from orbiter import __version__; print(__version__)')`
PYTHON := `which python || which python3`

default:
  @just --choose

# Print this help text
help:
    @just --list

# Install project and python dependencies (incl. pre-commit) locally
install EDITABLE='':
    {{ PYTHON }} -m pip install {{EDITABLE}} '.[{{EXTRAS}}]'

# Install pre-commit to local project
install-precommit: install
    pre-commit install

# Update the baseline for detect-secrets / pre-commit
update-secrets:
    detect-secrets scan  > .secrets.baseline  # pragma: allowlist secret

# Run pytests with config from pyproject.toml
test:
    {{ PYTHON }} -m pytest -c pyproject.toml

# Test and emit a coverage report
test-with-coverage:
    {{ PYTHON }} -m pytest -c pyproject.toml --cov=./ --cov-report=xml

# Run integration tests
test-integration $MANUAL_TESTS="true":
    @just test

# Run ruff and black (normally done with pre-commit)
lint:
    ruff check .

# Render and serve documentation locally
serve-docs:
    mkdocs serve -w {{DOCS_DIR}} -w {{SRC_DIR}}

# Build documentation locally (likely unnecessary)
build-docs: clean
    mkdocs build

# Deploy documentation to GitHub pages (GHA does this automatically)
deploy-docs UPSTREAM="origin": clean
    mkdocs gh-deploy -r {{UPSTREAM}} --force

# Remove temporary or build folders
clean:
    rm -rf build dist site *.egg-info *.pyz orbiter-* workflow output
    find . | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf

# Tag as v$(<src>.__version__) and push to Github
tag:
    # Delete tag if it already exists
    git tag -d v{{VERSION}} || true
    # Tag and push
    git tag v{{VERSION}}

# Push tag to Github
deploy-tag: tag
    git push origin v{{VERSION}}

# Push tag to Github
deploy: deploy-tag

# Build the project
build: install clean
    {{ PYTHON }} -m build

# Package the `orbiter` binary
build-binary: clean
  {{ PYTHON }} -m PyInstaller --onefile --noconfirm --clean --specpath dist --name astronomer-orbiter \
    --collect-all orbiter \
    --hidden-import tzdata \
    --recursive-copy-metadata astronomer-orbiter \
    orbiter/__main__.py
  cp dist/astronomer-orbiter orbiter-$(uname -s | awk '{print tolower($0)}' )-$(uname -m)

docker-build-binary:
    #!/usr/bin/env bash
    set -euxo pipefail
    cat <<"EOF" | docker run --platform linux/amd64 -v `pwd`:/data -w /data -i ubuntu /bin/bash
    apt update && \
    apt install --yes python3 just pip && \
    just install --break-system-packages && \
    just build-binary
    EOF

docker-run-binary REPO='orbiter-community-translations'  DEMO="https://raw.githubusercontent.com/astronomer/orbiter-community-translations/refs/heads/main/tests/control_m/demo/workflow/demo.xml" RULESET='orbiter_translations.control_m.xml_demo.translation_ruleset' PLATFORM="linux/amd64":
    #!/usr/bin/env bash
    set -euxo pipefail
    cat <<"EOF" | docker run --platform {{PLATFORM}} -v `pwd`:/data -w /data -i ubuntu /bin/bash
    echo "[SETUP]" && \
    echo "setting up certificates for https" && \
    apt update && apt install -y curl ca-certificates && update-ca-certificates --fresh && \
    echo "sourcing .env" && \
    set -a && source .env && set +a && \
    chmod +x ./orbiter-linux-x86_64 && \
    echo "downloading {{DEMO}}" && \
    mkdir -p workflow && pushd workflow && \
    curl -ssLO {{DEMO}} && popd && \
    echo "[INSTALL]" && \
    echo "[ORBITER LIST-RULESETS]" && \
    ./orbiter-linux-x86_64 list-rulesets && \
    mkdir -p workflow && \
    echo "[ORBITER INSTALL]" && \
    LOG_LEVEL=DEBUG ./orbiter-linux-x86_64 install --repo={{REPO}} && \
    echo "[ORBITER TRANSLATE]" && \
    LOG_LEVEL=DEBUG ./orbiter-linux-x86_64 translate workflow/ output/ --ruleset {{RULESET}} --no-format && \
    echo "[ORBITER DOCUMENT]" && \
    echo "skipping d/t unknown error: No such file or directory: '/tmp/_MEICKUbts/mkdocs/templates'"
    EOF

docker-run-python REPO='orbiter-community-translations' DEMO="https://raw.githubusercontent.com/astronomer/orbiter-community-translations/refs/heads/main/tests/control_m/demo/workflow/demo.xml" RULESET='orbiter_translations.control_m.xml_demo.translation_ruleset' PLATFORM="linux/amd64":
    #!/usr/bin/env bash
    set -euxo pipefail
    cat <<"EOF" | docker run --platform {{PLATFORM}} -v `pwd`:/data -w /data -i python /bin/bash
    echo "[SETUP]" && \
    echo "sourcing .env" && \
    set -a && source .env && set +a && \
    echo "installing cargo (pendulum needs rust)" && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    export PATH="$HOME/.cargo/bin:$PATH" && \
    echo "downloading {{DEMO}}" && \
    mkdir -p workflow && pushd workflow && \
    curl -ssLO {{DEMO}} && popd && \
    echo "[INSTALL]" && \
    echo "installing orbiter" && \
    pip install '.'
    echo "[ORBITER LIST-RULESETS]" && \
    orbiter list-rulesets && \
    mkdir -p workflow && \
    echo "[ORBITER INSTALL]" && \
    LOG_LEVEL=DEBUG orbiter install --repo={{REPO}} && \
    echo "[ORBITER TRANSLATE]" && \
    LOG_LEVEL=DEBUG orbiter translate workflow/ output/ --ruleset {{RULESET}} && \
    echo "[ORBITER DOCUMENT]" && \
    LOG_LEVEL=DEBUG orbiter document --ruleset {{RULESET}} && \
    head translation_ruleset.html
    EOF

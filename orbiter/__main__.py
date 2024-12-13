from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Literal

import requests
import rich_click as click
from loguru import logger
from questionary import Choice, select
from rich.prompt import Prompt
from rich.console import Console
from tabulate import tabulate
from csv import DictReader
from rich.markdown import Markdown
import pkgutil
from urllib.request import urlretrieve

from orbiter import import_from_qualname
from orbiter.config import (
    RUNNING_AS_BINARY,
    KG_ACCOUNT_ID,
    TRANSLATION_VERSION,
    LOG_LEVEL,
)
from orbiter.rules.rulesets import TranslationRuleset


# ### LOGGING ###
def formatter(r):
    return (
        "<lvl>"
        + (  # add [time] WARN, etc. if it's not INFO
            "[{time:HH:mm:ss}|{level}] " if r["level"].no != logging.INFO else "[{time:HH:mm:ss}] "
        )
        + "{message}</>\n{exception}"  # add exception, if there is one
    )


logger.remove()
sys.tracebacklimit = 1000 if LOG_LEVEL == "DEBUG" else 0
logger_defaults = dict(colorize=True, format=formatter)
exceptions_off = {"backtrace": False, "diagnose": False}
exceptions_on = {"backtrace": True, "diagnose": True}
logger.add(
    sys.stdout,
    level=LOG_LEVEL,
    **logger_defaults,
    **(exceptions_off if LOG_LEVEL != "DEBUG" else exceptions_on),
)

INPUT_DIR_ARGS = ("input-dir",)
INPUT_DIR_KWARGS = dict(
    type=click.Path(
        exists=True,
        dir_okay=True,
        file_okay=False,
        readable=True,
        resolve_path=True,
        path_type=Path,
    ),
    default=Path.cwd() / "workflow",
    required=True,
)
RULESET_ARGS = (
    "-r",
    "--ruleset",
)
RULESET_KWARGS = dict(
    help="Qualified name of a TranslationRuleset",
    type=str,
    prompt="Ruleset to use? (e.g. orbiter_community_translations.dag_factory.translation_ruleset)",
    required=True,
)


def import_ruleset(ruleset: str) -> TranslationRuleset:
    if RUNNING_AS_BINARY:
        _add_pyz()

    logger.debug(f"Importing ruleset: {ruleset}")
    (_, translation_ruleset) = import_from_qualname(ruleset)
    if not isinstance(translation_ruleset, TranslationRuleset):
        raise RuntimeError(f"translation_ruleset={translation_ruleset} is not a TranslationRuleset")
    return translation_ruleset


def run(cmd: str, **kwargs):
    """Helper method to run a command and log the output"""
    from loguru import logger

    output = subprocess.run(cmd, **kwargs)
    if getattr(output, "stdout", False):
        logger.info(output.stdout)
    if getattr(output, "stderr", False):
        logger.warning(output.stderr)
    return output


def run_ruff_formatter(output_dir: Path):
    logger.info("Reformatting output...")
    changed_files = output_dir
    # noinspection PyBroadException
    try:
        # noinspection PyUnresolvedReferences
        import git

        changed_files = " ".join(
            (
                file
                for file in git.Repo(output_dir).git.diff(output_dir, name_only=True).split("\n")
                if file.endswith(".py")
            )
        )
    except ImportError:
        logger.debug("Unable to acquire list of changed files in output directory, reformatting output directory...")
    except Exception:
        logger.debug("Unable to acquire list of changed files in output directory, reformatting output directory...")

    output = run(
        f"ruff check --select E,F,UP,B,SIM,I --ignore E501,SIM117,SIM101 --fix {changed_files}",
        shell=True,
        text=True,
        capture_output=True,
    )
    if output.returncode != 0:
        click.echo("Ruff encountered an error!")
        raise click.Abort()

    run(
        f"ruff format {changed_files}",
        shell=True,
        text=True,
        check=True,
        capture_output=True,
    )


@click.group(
    context_settings={"auto_envvar_prefix": "ORBITER"},
    epilog="Check out https://astronomer.github.io/orbiter for more details",
)
@click.version_option(package_name="astronomer-orbiter", prog_name="orbiter")
def orbiter():
    """
    Orbiter is a CLI that converts other workflows to Airflow Projects.
    """


@orbiter.command()
@click.argument(*INPUT_DIR_ARGS, **INPUT_DIR_KWARGS)
@click.argument(
    "output-dir",
    type=click.Path(
        dir_okay=True,
        file_okay=False,
        readable=True,
        resolve_path=True,
        path_type=Path,
    ),
    default=Path.cwd() / "output",
    required=True,
)
@click.option(*RULESET_ARGS, **RULESET_KWARGS)
@click.option(
    "--format/--no-format",
    "_format",
    help="[optional] format the output with Ruff",
    default=True,
    show_default=True,
)
def translate(
    input_dir: Path,
    output_dir: Path,
    ruleset: str | None,
    _format: bool,
):
    """Translate workflows in an `INPUT_DIR` to an `OUTPUT_DIR` Airflow Project.

    Provide a specific ruleset with the `--ruleset` flag.

    Run `orbiter list-rulesets` to see available rulesets.

    `INPUT_DIR` defaults to `$CWD/workflow`.

    `OUTPUT_DIR` defaults to `$CWD/output`

    Formats output with Ruff (https://astral.sh/ruff), by default.
    """
    logger.debug(f"Creating output directory {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    translation_ruleset = import_ruleset(ruleset)
    try:
        translation_ruleset.translate_fn(translation_ruleset=translation_ruleset, input_dir=input_dir).render(
            output_dir
        )
    except RuntimeError as e:
        logger.error(f"Error encountered during translation: {e}")
        raise click.Abort()
    if _format:
        run_ruff_formatter(output_dir)


@orbiter.command()
@click.argument(*INPUT_DIR_ARGS, **INPUT_DIR_KWARGS)
@click.option(*RULESET_ARGS, **RULESET_KWARGS)
@click.option(
    "--format",
    "_format",
    type=click.Choice(["json", "csv", "md"]),
    default="md",
    help="[optional] format for analysis output",
    show_default=True,
)
@click.option(
    "-o",
    "--output-file",
    type=click.File("w", lazy=True),
    default="-",
    show_default=True,
    help="File to write to, defaults to '-' (stdout)",
)
def analyze(
    input_dir: Path,
    ruleset: str | None,
    _format: Literal["json", "csv", "md"],
    output_file: Path | None,
):
    """Analyze workflows in an `INPUT_DIR`

    Provide a specific ruleset with the `--ruleset` flag.

    Run `orbiter list-rulesets` to see available rulesets.

    `INPUT_DIR` defaults to `$CWD/workflow`.
    """
    if isinstance(output_file, Path):
        output_file = output_file.open("w", newline="")
    translation_ruleset = import_ruleset(ruleset)
    try:
        translation_ruleset.translate_fn(translation_ruleset=translation_ruleset, input_dir=input_dir).analyze(
            _format, output_file
        )
    except RuntimeError as e:
        logger.exception(f"Error encountered during translation: {e}")
        raise click.Abort()


def _pip_install(repo: str, key: str):
    """If we are running via python/pip, we can utilize pip to install translations"""
    _exec = f"{sys.executable} -m pip install {repo}"
    _exec += f"=={TRANSLATION_VERSION}" if TRANSLATION_VERSION != "latest" else ""
    if repo == "astronomer-orbiter-translations":
        if not key:
            raise ValueError("License key is required for 'astronomer-orbiter-translations'!")
        extra = f' --index-url "https://license:{key}@api.keygen.sh/v1/accounts/{KG_ACCOUNT_ID}/engines/pypi/simple"'
        _exec = f"{_exec}{extra}"
    logger.debug(_exec.replace(key or "<nothing>", "****"))
    output = run(_exec, shell=True, text=True, capture_output=True)
    if output.returncode != 0:
        click.echo(f"Encountered an error installing translation rulesets from {repo}!")
        raise click.Abort()


def _get_keygen_pyz(key):
    url = f"https://api.keygen.sh/v1/accounts/{KG_ACCOUNT_ID}/releases/latest/artifacts"
    logger.debug(f"Finding latest release from '{url}'")
    r = requests.get(url, auth=("license", key), timeout=60)
    r.raise_for_status()
    try:
        latest_orbiter_translations_pyz_id = next(
            artifact["id"]
            for artifact in r.json().get("data", [])
            if artifact.get("attributes", {}).get("filename") == "orbiter_translations.pyz"
        )
    except StopIteration:
        raise ValueError("No Artifact found with filename='orbiter_translations.pyz'")
    url = (
        f"https://api.keygen.sh/v1/accounts/{KG_ACCOUNT_ID}"
        f"/releases/latest/artifacts/{latest_orbiter_translations_pyz_id}"
    )
    r = requests.get(url, auth=("license", key), timeout=60)
    r.raise_for_status()
    logger.debug(f"Fetching translations .pyz from {url}")
    with open("astronomer_orbiter_translations.pyz", "wb") as f:
        f.write(r.content)


def _get_gh_pyz(
    repo: str = "https://github.com/astronomer/orbiter-community-translations",
    file: str = "orbiter_translations.pyz",
):
    if TRANSLATION_VERSION != "latest":
        url = f"{repo}/releases/download/{TRANSLATION_VERSION}/{file}"
    else:
        url = f"{repo}/releases/latest/download/{file}"
    logger.info(f"Downloading {file} from {url}")
    (downloaded_file, res) = urlretrieve(url, file)  # nosec B310
    logger.debug(f"Downloaded {file} to {downloaded_file}, response: {res}")
    return downloaded_file


def _add_pyz():
    logger.debug(f"Adding current directory {os.getcwd()} to sys.path")
    sys.path.insert(0, os.getcwd())

    local_pyz = [str(_path.resolve()) for _path in Path(".").iterdir() if _path.suffix == ".pyz"]
    logger.debug(f"Adding local .pyz files {local_pyz} to sys.path")
    sys.path += local_pyz


def _bin_install(repo: str, key: str):
    """If we are running via a PyInstaller binary, we need to download a .pyz"""
    if "astronomer-orbiter-translations" in repo:
        if not key:
            raise ValueError("License key is required for 'astronomer-orbiter-translations'!")
        _get_keygen_pyz(key)
    else:
        _get_gh_pyz()
    _add_pyz()
    (_, _version) = import_from_qualname("orbiter_translations.version")
    logger.info(f"Successfully installed {repo}, version: {_version}")


# noinspection t
@orbiter.command()
@click.option(
    "-r",
    "--repo",
    type=click.Choice(["astronomer-orbiter-translations", "orbiter-community-translations"]),
    required=False,
    allow_from_autoenv=True,
    show_envvar=True,
    help="Choose a repository to install (will prompt, if not given)",
)
@click.option(
    "-k",
    "--key",
    help="[Optional] License Key to use for the translation ruleset. Should look like "
    "`AAAA-BBBB-1111-2222-3333-XXXX-YYYY-ZZZZ`",
    type=str,
    default=None,
    allow_from_autoenv=True,
    show_envvar=True,
)
def install(
    repo: (Literal["astronomer-orbiter-translations", "orbiter-community-translations"] | None),
    key: str | None,
):
    """Install a new Translation Ruleset from a repository"""
    if not repo:
        choices = [
            "astronomer-orbiter-translations",
            "orbiter-community-translations",
            "Other",
        ]
        if (
            repo := select(
                message="Which repository would you like to install?",
                choices=[Choice(title=choice, value=choice) for choice in choices],
                use_indicator=True,
                use_shortcuts=True,
                show_selected=True,
                default="orbiter-community-translations",
            ).ask()
        ) == "Other":
            repo = None
            while not repo:
                repo = Prompt.ask("Package Name or Repository URL (e.g. git+https://github.com/my/repo.git )")

    if RUNNING_AS_BINARY:
        _bin_install(repo, key)
    else:
        _pip_install(repo, key)


# noinspection PyShadowingBuiltins
@orbiter.command(help="List available Translation Rulesets")
def list_rulesets():
    console = Console()

    table = tabulate(
        list(DictReader(pkgutil.get_data("orbiter.assets", "supported_origins.csv").decode().splitlines())),
        headers="keys",
        tablefmt="pipe",
        # https://github.com/Textualize/rich/issues/3027
        missingval="⠀",  # (special 'braille space' character)
    )
    console.print(
        Markdown(
            f"# Available Origins\n{table}\n\n"
            "More info available in our docs: https://astronomer.github.io/orbiter/origins/#supported-origins",
            style="magenta",
        )
    )


@orbiter.command(
    help="Write Translation module documentation as HTML. Translations must already be installed with `orbiter install`"
)
@click.option(
    *RULESET_ARGS,
    multiple=True,
    required=True,
    prompt="Translation module to document? (e.g. `orbiter_translation.oozie.xml_demo`)",
    help="Translation module to document (e.g `orbiter_translation.oozie.xml_demo`), can be supplied multiple times",
)
@click.option(
    "--output-file",
    "-o",
    type=click.File(mode="w", atomic=True, lazy=True),
    default="translation_ruleset.html",
    help="HTML file to write to. Defaults to `translation_ruleset.html`. Use `-` to write to stdout",
    show_default=True,
)
def document(ruleset: list[str], output_file):
    from mkdocs.commands import build
    from mkdocs.config.defaults import MkDocsConfig
    import tempfile
    import htmlark
    from io import StringIO

    index_md = """# Documentation\n"""
    for rs in ruleset:
        # remove .translation_ruleset, if it's the end of the input
        rs = re.sub(r"\.translation_ruleset$", "", rs)
        index_md += f"::: {rs}\n"

    with tempfile.TemporaryDirectory() as tmpdir_read, tempfile.TemporaryDirectory() as tmpdir_write:
        logger.debug(f"Writing index.md to {tmpdir_read}")
        (Path(tmpdir_read) / "index.md").write_text(index_md)

        with StringIO(f"""
site_name: Translation Documentation
docs_dir: {tmpdir_read}
site_dir: {tmpdir_write}
theme:
  name: material
  palette:
    scheme: slate
  features:
    - toc.integrate
    - content.code.copy
markdown_extensions:
  - pymdownx.magiclink
  - pymdownx.superfences:
  - pymdownx.saneheaders
  - pymdownx.highlight:
      use_pygments: true
      anchor_linenums: true
  - pymdownx.inlinehilite
  - admonition
  - pymdownx.details
plugins:
- mkdocstrings:
    handlers:
      python:
        options:
          docstring_style: sphinx
          show_root_heading: true
          separate_signature: true
          show_signature_annotations: true
          signature_crossrefs: true
          unwrap_annotated: true
          show_object_full_path: true
          show_symbol_type_toc: true
          show_symbol_type_heading: true
          show_if_no_docstring: true
          merge_init_into_class: true
          summary: true
          group_by_category: true
          show_bases: false
""") as f:
            # Copypaste from mkdocs.commands.build
            logger.info("Building documentation...")
            logger.debug("Injecting mkdocs config directly...")
            cfg = MkDocsConfig(config_file_path=None)
            cfg.load_file(f)
            cfg.load_dict({})
            cfg.validate()
            cfg.plugins.on_startup(command="build", dirty=False)
            logger.debug(f"Building mkdocs to {tmpdir_write}/ ...")
            try:
                build.build(cfg, dirty=False)
            finally:
                cfg.plugins.on_shutdown()

            # Use htmlark to convert the page to a single HTML file with css/js/etc included inline
            logger.info(f"Writing documentation to {output_file.name}...")
            output_file.write(htmlark.convert_page(f"{tmpdir_write}/index.html", ignore_errors=True))


if __name__ == "__main__":
    orbiter()

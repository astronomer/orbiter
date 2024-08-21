from __future__ import annotations

import logging
import os
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

from orbiter import import_from_qualname, KG_ACCOUNT_ID
from orbiter.rules.rulesets import TranslationRuleset

RUNNING_AS_BINARY = getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")


# ### LOGGING ###
def formatter(r):
    return (
        "<lvl>"
        + (  # add [time] WARN, etc. if it's not INFO
            "[{time:HH:mm:ss}|{level}] "
            if r["level"].no != logging.INFO
            else "[{time:HH:mm:ss}] "
        )
        + "{message}</>\n{exception}"  # add exception, if there is one
    )


logger.remove()
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
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
                for file in git.Repo(output_dir)
                .git.diff(output_dir, name_only=True)
                .split("\n")
                if file.endswith(".py")
            )
        )
    except ImportError:
        logger.debug(
            "Unable to acquire list of changed files in output directory, reformatting output directory..."
        )
    except Exception:
        logger.debug(
            "Unable to acquire list of changed files in output directory, reformatting output directory..."
        )

    output = run(
        f"ruff check --select E,F,UP,B,SIM,I --ignore E501 --fix {changed_files}",
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
@click.argument(
    "input-dir",
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
@click.option(
    "-r",
    "--ruleset",
    help="Qualified name of a TranslationRuleset",
    type=str,
    prompt="Ruleset to use? (e.g. orbiter_community_translations.dag_factory.translation_ruleset)",
    required=True,
)
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

    Run `orbiter help` to see available rulesets.

    `INPUT_DIR` defaults to `$CWD/workflow`.

    `OUTPUT_DIR` defaults to `$CWD/output`
    """
    logger.debug(f"Creating output directory {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    sys.path.insert(0, os.getcwd())
    logger.debug(f"Adding current directory {os.getcwd()} to sys.path")

    if RUNNING_AS_BINARY:
        _add_pyz()

    logger.debug(f"Importing ruleset: {ruleset}")
    (_, translation_ruleset) = import_from_qualname(ruleset)
    if not isinstance(translation_ruleset, TranslationRuleset):
        raise RuntimeError(
            f"translation_ruleset={translation_ruleset} is not a TranslationRuleset"
        )

    try:
        translation_ruleset.translate_fn(
            translation_ruleset=translation_ruleset, input_dir=input_dir
        ).render(output_dir)
    except RuntimeError as e:
        logger.error(f"Error encountered during translation: {e}")
        raise click.Abort()
    if _format:
        run_ruff_formatter(output_dir)


def _pip_install(repo: str, key: str):
    """If we are running via python/pip, we can utilize pip to install translations"""
    _exec = f"pip3 install {repo}"
    if repo == "astronomer-orbiter-translations":
        if not key:
            raise ValueError(
                "License key is required for 'astronomer-orbiter-translations'!"
            )
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
            if artifact.get("attributes", {}).get("filename")
            == "orbiter_translations.pyz"
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


def _add_pyz():
    local_pyz = [
        str(_path.resolve()) for _path in Path(".").iterdir() if _path.suffix == ".pyz"
    ]
    logger.debug(f"Adding local .pyz files {local_pyz} to sys.path")
    sys.path += local_pyz


def _bin_install(repo: str, key: str):
    """If we are running via a PyInstaller binary, we need to download a .pyz"""
    if repo == "astronomer-orbiter-translations":
        if not key:
            raise ValueError(
                "License key is required for 'astronomer-orbiter-translations'!"
            )
        _get_keygen_pyz(key)
    else:
        raise NotImplementedError()
    _add_pyz()
    (_, _version) = import_from_qualname("orbiter_translations.version")
    logging.info(f"Successfully installed {repo}, version: {_version}")


# noinspection t
@orbiter.command()
@click.option(
    "-r",
    "--repo",
    type=click.Choice(
        ["astronomer-orbiter-translations", "orbiter-community-translations"]
    ),
    required=False,
    allow_from_autoenv=True,
    show_envvar=True,
    help="Choose a repository to install (will prompt, if not given)",
)
@click.option(
    "-k",
    "--key",
    help="[Optional] License Key to use for the translation ruleset.\n\n"
    "Should look like 'AAAA-BBBB-1111-2222-3333-XXXX-YYYY-ZZZZ'",
    type=str,
    default=None,
    allow_from_autoenv=True,
    show_envvar=True,
)
def install(
    repo: (
        Literal["astronomer-orbiter-translations", "orbiter-community-translations"]
        | None
    ),
    key: str | None,
):
    """Install a new Orbiter Translation Ruleset from a repository"""
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
                repo = Prompt.ask(
                    "Package Name or Repository URL (e.g. git+https://github.com/my/repo.git )"
                )

    if RUNNING_AS_BINARY:
        _bin_install(repo, key)
    else:
        _pip_install(repo, key)


# noinspection PyShadowingBuiltins
@orbiter.command(help="List available Translation Rulesets")
def help():
    console = Console()

    table = tabulate(
        list(
            DictReader(
                pkgutil.get_data("orbiter.assets", "supported_origins.csv")
                .decode()
                .splitlines()
            )
        ),
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


if __name__ == "__main__":
    orbiter()

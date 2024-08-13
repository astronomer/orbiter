from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

import click
from loguru import logger

from orbiter import import_from_qualname
from orbiter.rules.rulesets import TranslationRuleset


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


# noinspection PyUnresolvedReferences
def run_ruff_formatter(output_dir: Path):
    logger.info("Reformatting output...")
    changed_files = output_dir
    # noinspection PyBroadException
    try:
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

    output = subprocess.run(
        f"ruff check --select E,F,UP,B,SIM,I --ignore E501 --fix {changed_files}",
        shell=True,
        text=True,
        capture_output=True,
    )
    if output.stdout:
        logger.info(output.stdout)
    if output.stderr:
        logger.warning(output.stderr)
    if output.returncode != 0:
        click.echo("Ruff encountered an error!")
        raise click.Abort()

    output = subprocess.run(
        f"ruff format {changed_files}",
        shell=True,
        text=True,
        check=True,
        capture_output=True,
    )
    if output.stdout:
        logger.info(output.stdout)
    if output.stderr:
        logger.warning(output.stderr)


@click.group(epilog="Check out https://astronomer.github.io/orbiter for more details")
@click.version_option(package_name="astronomer-orbiter", prog_name="orbiter")
def orbiter():
    """
    `orbiter` is a CLI that runs on your workstation
    and converts workflow definitions from other tools to Airflow Projects.
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
    prompt="Ruleset to use? (e.g. my_module.my_ruleset or orbiter_community_translations.dag_factory.translation_ruleset)",  # noqa: E501l
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
    """
    Translate workflow artifacts in an `INPUT_DIR` folder
    to an `OUTPUT_DIR` Airflow Project folder.

    Provide a specific ruleset with the `--ruleset` flag.

    `INPUT_DIR` defaults to `$CWD/workflow`

    `OUTPUT_DIR` defaults to `$CWD/output`
    """
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    # Get Translation Ruleset
    sys.path.insert(0, os.getcwd())
    (_, translation_ruleset) = import_from_qualname(ruleset)
    if not isinstance(translation_ruleset, TranslationRuleset):
        raise RuntimeError(
            f"translation_ruleset={translation_ruleset} is not a TranslationRuleset"
        )

    translation_ruleset.translate_fn(
        translation_ruleset=translation_ruleset, input_dir=input_dir
    ).render(output_dir)
    if _format:
        run_ruff_formatter(output_dir)


if __name__ == "__main__":
    orbiter()

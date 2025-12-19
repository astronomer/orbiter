from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Tuple, Mapping, List, Annotated

from loguru import logger
from pydantic import AfterValidator

from orbiter.config import TRIM_LOG_OBJECT_LENGTH

__version__ = "1.10.1"

version = __version__


def clean_value(s: str):
    """Cleans a string to be a standard value, such as one that might be a python variable name

    Contains similar logic to
    https://github.com/apache/airflow/blob/279d1f7c6483910906a567d8416cd2f230c9df31/airflow/utils/helpers.py#L50

    - Changes - to _
    - Changes spaces to _
    - Strips anything non-alphanumeric and not _ or .
    - Adds an "n" prefix if it starts with a number.
    - snake_cases the string

    ```pycon
    >>> clean_value("MyTaskId")
    'my_task_id'
    >>> clean_value("my-Task ID")
    'my_task_id'
    >>> clean_value("01 My Task")
    'n01_my_task'

    ```
    """
    import inflection

    s = s[:250]  # shorten the string to 250 chars max
    s = inflection.underscore(s)  # swap to snake_case
    s = re.sub("[ -]", "_", s)  # swap space or dash to _
    s = re.sub("[^A-Za-z0-9_.]", "", s)  # remove anything that isn't alphanum or _ or .
    s = re.sub(r"^(\d)", r"n\1", s)  # prefix a number at the start with an "n"
    return s


def insert_cwd_to_sys_path():
    """Insert the current directory to `sys.path`, if it is not already there.

    This is used for finding translation rulesets locally (or as a `.pyz`, locally)

    ```pycon
    >>>  # setup - del from sys.path if it's there
    ... path = str(Path.cwd()); sys.path = [p for p in sys.path if p != path]
    >>> path in sys.path
    False
    >>> insert_cwd_to_sys_path()
    >>> path in sys.path
    True
    """
    cwd = Path.cwd()
    logger.debug(f"Adding current directory {cwd} to sys.path")
    if str(cwd) not in sys.path:
        sys.path.insert(0, str(cwd))


def import_from_qualname(qualname) -> Tuple[str, Any]:
    """Import a function or module from a qualified name
    :param qualname: The qualified name of the function or module to import (e.g. a.b.d.MyOperator or json)
    :return Tuple[str, Any]: The name of the function or module, and the function or module itself
    >>> import_from_qualname("json.loads")
    ('loads', <function loads at ...>)
    >>> import_from_qualname("json")
    ('json', <module 'json' from '...'>)
    """
    from importlib import import_module

    [module, name] = qualname.rsplit(".", 1) if "." in qualname else [qualname, qualname]
    imported_module = import_module(module)
    return (
        name,
        getattr(imported_module, name) if "." in qualname else imported_module,
    )


def _backport_walk(input_dir: Path):
    """Path.walk() is only available in Python 3.12+, so, backport"""
    import os

    for result in os.walk(input_dir):
        yield Path(result[0]), result[1], result[2]


def trim_dict(v):
    """Stringify and trim a dictionary if it's greater than a certain length
    (used to trim down overwhelming log output)"""
    if TRIM_LOG_OBJECT_LENGTH != -1 and isinstance(v, Mapping):
        if len(str(v)) > TRIM_LOG_OBJECT_LENGTH:
            return json.dumps(v, default=str)[:TRIM_LOG_OBJECT_LENGTH] + "..."
    if isinstance(v, list):
        return [trim_dict(_v) for _v in v]
    return v


def validate_qualified_imports(qualified_imports: List[str]) -> List[str]:
    """
    ```pycon
    >>> validate_qualified_imports(["json", "package.module.Class"])
    ['json', 'package.module.Class']

    ```
    """
    for _qualname in qualified_imports:
        assert qualname_validator.match(_qualname), (
            f"Import Qualified Name='{_qualname}' is not valid."
            f"Qualified Names must match regex {qualname_validator_regex}"
        )
    return qualified_imports


QualifiedImport = Annotated[str, AfterValidator(validate_qualified_imports)]

qualname_validator_regex = r"^[\w.]+$"
qualname_validator = re.compile(qualname_validator_regex)

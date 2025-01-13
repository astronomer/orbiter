from __future__ import annotations

import re
from typing import Any, Tuple

__version__ = "1.4.2"

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


if __name__ == "__main__":
    import doctest

    doctest.testmod(optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.IGNORE_EXCEPTION_DETAIL)

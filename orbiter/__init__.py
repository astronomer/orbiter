from __future__ import annotations

import re
from enum import Enum

__version__ = "1.0.0-alpha4"

from typing import Any, Tuple

import inflection

version = __version__


class FileType(Enum):
    YAML = "YAML"
    XML = "XML"
    JSON = "JSON"


# noinspection t
def xmltodict_parse(input_str: str) -> Any:
    """Calls `xmltodict.parse` and does post-processing fixes.

    `xmltodict.parse` returns EITHER
    - a dict (one child element of type)
    - or a list of dict (many child element of type)
    which is very confusing.

    This is an issue with the xml 'spec' they are referencing.
    This standardizes it to the latter case, by recursively descending.

    MEANING - any elements will be a list of dict, even if there's just one of the element

    ```pycon
    >>> xmltodict_parse("")
    Traceback (most recent call last):
    xml.parsers.expat.ExpatError: no element found: line 1, column 0
    >>> xmltodict_parse("<a></a>")
    {'a': None}
    >>> xmltodict_parse("<a foo='bar'></a>")
    {'a': [{'@foo': 'bar'}]}
    >>> xmltodict_parse("<a foo='bar'><foo bar='baz'></foo></a>")  # Singleton - gets modified
    {'a': [{'@foo': 'bar', 'foo': [{'@bar': 'baz'}]}]}
    >>> xmltodict_parse("<a foo='bar'><foo bar='baz'><bar><bop></bop></bar></foo></a>")  # Nested Singletons - modified
    {'a': [{'@foo': 'bar', 'foo': [{'@bar': 'baz', 'bar': [{'bop': None}]}]}]}
    >>> xmltodict_parse("<a foo='bar'><foo bar='baz'></foo><foo bing='bop'></foo></a>")
    {'a': [{'@foo': 'bar', 'foo': [{'@bar': 'baz'}, {'@bing': 'bop'}]}]}

    ```

    :param input_str: The XML string to parse
    :type input_str: str
    :return: The parsed XML
    :rtype: dict
    """
    import xmltodict

    # noinspection t
    def _fix(d):
        """fix the dict in place, recursively, standardizing on a list of dict even if there's only one entry."""
        # if it's a dict, descend to fix
        if isinstance(d, dict):
            for k, v in d.items():
                # @keys are properties of elements, non-@keys are elements
                if not k.startswith("@"):
                    if isinstance(v, dict):
                        # THE FIX
                        # any non-@keys should be a list of dict, even if there's just one of the element
                        d[k] = [v]
                        _fix(v)
                    else:
                        _fix(v)
        # if it's a list, descend to fix
        if isinstance(d, list):
            for v in d:
                _fix(v)

    output = xmltodict.parse(input_str)
    _fix(output)
    return output


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
    >>> import_from_qualname('json.loads')
    ('loads', <function loads at ...>)
    >>> import_from_qualname('json')
    ('json', <module 'json' from '...'>)
    """
    from importlib import import_module

    [module, name] = (
        qualname.rsplit(".", 1) if "." in qualname else [qualname, qualname]
    )
    imported_module = import_module(module)
    return (
        name,
        getattr(imported_module, name) if "." in qualname else imported_module,
    )


if __name__ == "__main__":
    import doctest

    doctest.testmod(
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL
    )

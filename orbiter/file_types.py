from __future__ import annotations

import json
from abc import ABC
from functools import partial
from typing import Callable, Set, ClassVar, Any

import xmltodict
import yaml
from pydantic import (
    BaseModel,
)
from pydantic.v1 import validator

from jilutil.jil_parser import JilParser


class FileType(BaseModel, ABC, arbitrary_types_allowed=True):
    """**Abstract Base** File Type

    :param extension: The file extension(s) for this file type
    :type extension: Set[str]
    :param load_fn: The function to load the file into a dictionary for this file type
    :type load_fn: Callable[[str], dict]
    :param dump_fn: The function to dump a dictionary to a string for this file type
    :type dump_fn: Callable[[dict], str]
    """

    extension: ClassVar[Set[str]]
    load_fn: ClassVar[Callable[[str], dict]]
    dump_fn: ClassVar[Callable[[dict], str]]

    def __hash__(self):
        return hash(tuple(self.extension))

    @validator("extension", pre=True)
    @classmethod
    def ext_validate(cls, v: Set[str]):
        if not v:
            raise ValueError("Extension cannot be an empty set")
        for ext in v:
            if not isinstance(ext, str):
                raise ValueError("Extension should be a string")
            if "." in v:
                raise ValueError("Extension should not contain '.'")
        return {ext.lower() for ext in v}


class FileTypeJSON(FileType):
    """JSON File Type

    ```pycon
    >>> out = FileTypeJSON.dump_fn({'a': 1}); out
    '{"a": 1}'
    >>> FileTypeJSON.load_fn(out)
    {'a': 1}

    ```
    :param extension: JSON
    :type extension: Set[str]
    :param load_fn: json.loads
    :type load_fn: Callable[[str], dict]
    :param dump_fn: json.dumps
    :type dump_fn: Callable[[dict], str]
    """

    extension: ClassVar[Set[str]] = {"JSON"}
    load_fn: ClassVar[Callable[[str], dict]] = json.loads
    dump_fn: ClassVar[Callable[[dict], str]] = partial(json.dumps, default=str)


# noinspection t
def xmltodict_parse(input_str: str) -> Any:
    """Calls `xmltodict.parse` and does post-processing fixes.

    !!! note

        The original [`xmltodict.parse`](https://pypi.org/project/xmltodict/) method returns EITHER:

        - a dict (one child element of type)
        - or a list of dict (many child element of type)

        This behavior can be confusing, and is an issue with the original xml spec being referenced.

        **This method deviates by standardizing to the latter case (always a `list[dict]`).**

        **All XML elements will be a list of dictionaries, even if there's only one element.**

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


class FileTypeXML(FileType):
    """XML File Type

    !!! note
        This class uses a custom `xmltodict_parse` method to standardize the output to a list of dictionaries

    ```pycon
    >>> out = FileTypeXML.dump_fn({'a': 1}); out
    '<?xml version="1.0" encoding="utf-8"?>\\n<a>1</a>'
    >>> FileTypeXML.load_fn(out)
    {'a': '1'}

    ```
    :param extension: XML
    :type extension: Set[str]
    :param load_fn: xmltodict_parse
    :type load_fn: Callable[[str], dict]
    :param dump_fn: xmltodict.unparse
    :type dump_fn: Callable[[dict], str]
    """

    extension: ClassVar[Set[str]] = {"XML"}
    load_fn: ClassVar[Callable[[str], dict]] = xmltodict_parse
    dump_fn: ClassVar[Callable[[dict], str]] = xmltodict.unparse


class FileTypeYAML(FileType):
    """YAML File Type


    ```pycon
    >>> out = FileTypeYAML.dump_fn({'a': 1}); out
    'a: 1\\n'
    >>> FileTypeYAML.load_fn(out)
    {'a': 1}

    ```
    :param extension: YAML, YML
    :type extension: Set[str]
    :param load_fn: yaml.safe_load
    :type load_fn: Callable[[str], dict]
    :param dump_fn: yaml.safe_dump
    :type dump_fn: Callable[[dict], str]
    """

    extension: ClassVar[Set[str]] = {"YAML", "YML"}
    load_fn: ClassVar[Callable[[str], dict]] = yaml.safe_load
    dump_fn: ClassVar[Callable[[dict], str]] = yaml.safe_dump


def dump_jil(_: dict) -> str:
    raise NotImplementedError("JIL dumping is not implemented yet.")


class FileTypeJIL(FileType):
    """JIL File Type

    :param extension: JIL
    :type extension: Set[str]
    :param load_fn: custom JIL loading function
    :type load_fn: Callable[[str], dict]
    :param dump_fn: custom JIL dumping function
    :type dump_fn: Callable[[dict], str]
    """

    extension: ClassVar[Set[str]] = {"JIL"}
    load_fn: ClassVar[Callable[[str], dict]] = JilParser(None).parse_jobs_from_str
    dump_fn: ClassVar[Callable[[dict], str]] = dump_jil


if __name__ == "__main__":
    import doctest

    doctest.testmod(optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.IGNORE_EXCEPTION_DETAIL)

from __future__ import annotations

from typing import Set, Annotated

from pydantic import BaseModel, Field


class OrbiterMeta(BaseModel):
    """Records information about the rule that matched an input to create an object.

    :param matched_rule_source: The source code of the rule that matched the input.
    :type matched_rule_source: str | None
    :param matched_rule_docstring: The documentation of the rule that matched the input.
    :type matched_rule_docstring: str | None
    :param matched_rule_params_doc: The documentation of parameters that the rule consumed and emitted.
    :type matched_rule_params_doc: dict[str, str] | None
    :param matched_rule_name: The name of the rule that matched the input.
    :type matched_rule_name: str | None
    :param matched_rule_priority: The priority of the rule that matched the input.
    :type matched_rule_priority: int | None
    :param visited_keys: Keys that have been visited, dot separated for nested inputs.
    :type visited_keys: list[str] | None
    """

    matched_rule_source: Annotated[
        str | None, Field(default=None, description="The source code of the rule that matched the input.")
    ]
    matched_rule_docstring: Annotated[
        str | None, Field(default=None, description="The documentation of the rule that matched the input.")
    ]
    matched_rule_params_doc: Annotated[
        dict[str, str] | None,
        Field(default=None, description="The documentation of parameters that the rule consumed and emitted."),
    ]
    matched_rule_name: Annotated[
        str | None, Field(default=None, description="The name of the rule that matched the input.")
    ]
    matched_rule_priority: Annotated[
        int | None, Field(default=None, description="The priority of the rule that matched the input.")
    ]
    visited_keys: Annotated[
        list[str] | None,
        Field(default=None, description="Keys that have been visited, dot separated for nested inputs"),
    ]


class VisitTrackedDict(dict):
    """A Dictionary which tracks if a given key has been accessed via `__getitem__`

    ```pycon
    >>> v = VisitTrackedDict({"a": 1, "b": {"c": 2}, "d": 3})
    ... # when we initialize a dict with a child dict
    >>> v.get_visited()
    ... # nothing has been visited yet
    []
    >>> v["a"] = "foo"
    >>> v.get_visited()
    ... # when we assign a value to a key, it is not visited yet
    []
    >>> v["a"] and v.get_visited()
    ... # when we access 'a', it is added to the visited set
    ['a']
    >>> b: VisitTrackedDict = v.get("b", {})
    >>> v.get_visited()
    ... # when we pull 'b' out into a var, it is added to the visited set
    ['a', 'b']
    >>> b.get_visited()
    ... # nothing has been visited yet, for b
    []
    >>> v["b"]["c"] and v.get_visited(recursive=False)
    ... # when b.c is accessed but recursive=False, we still see only the top-level keys
    ['a', 'b']
    >>> b.get_visited()
    ... # the child keeps track of its visited items
    ['c']
    >>> v.get_visited(recursive=True)
    ... # recursive=True by default, dot prefixes children.
    ['a', 'b', 'b.c']
    >>> v = VisitTrackedDict({"a": 1, "b": 2, "c": 3, "d": 4})
    >>> for k, _v in v.items():
    ...     pass
    >>> v.get_visited()
    ... # iterating over .items() counts as visiting all entries
    ['a', 'b', 'c', 'd']
    >>> v = VisitTrackedDict({"a": 1, "b": 2, "c": 3, "d": 4})
    >>> for _v in v.values():
    ...     pass
    >>> v.get_visited()
    ... # iterating over .values() counts as visiting all entries
    ['a', 'b', 'c', 'd']
    >>> v = VisitTrackedDict({"a": 1, "b": 2, "c": 3, "d": 4})
    >>> for _v in v.values():
    ...     break
    >>> v.get_visited()
    ... # if we stop the loop part-way we didn't visit everything
    ['a']

    ```
    """

    def get_visited(self, recursive: bool = True, prefix: str = ""):
        _prefix = prefix + "." if prefix else prefix
        visited = {_prefix + k for k in self.__visited__}
        for k, v in super().items():
            if isinstance(v, VisitTrackedDict):
                visited.update(v.get_visited(recursive, prefix=_prefix + k) if recursive else {})
        return sorted(visited)

    def __init__(self, d: dict):
        self.__visited__: Set[str] = set()
        self.__data__ = d
        super().__init__({k: VisitTrackedDict(v) if isinstance(v, dict) else v for k, v in d.items()})

    def values(self):
        for k, v in super().items():
            self.__visited__.add(k)
            yield v

    def items(self):
        for k, v in super().items():
            self.__visited__.add(k)
            yield k, v

    def get(self, key, default=None):
        self.__visited__.add(key)
        return super().get(key, default)

    def __getitem__(self, key):
        self.__visited__.add(key)
        return super().__getitem__(key)

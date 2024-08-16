from __future__ import annotations

from abc import ABC
from typing import List, Annotated, Set

from pydantic import BaseModel, AfterValidator
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.connection import OrbiterConnection
from orbiter.objects.env_var import OrbiterEnvVar
from orbiter.objects.variable import OrbiterVariable
from orbiter.objects.include import OrbiterInclude


def validate_imports(v):
    assert len(v)
    for i in v:
        assert isinstance(i, OrbiterRequirement)
    return v


ImportList = Annotated[List[OrbiterRequirement], AfterValidator(validate_imports)]


class OrbiterBase(BaseModel, ABC, arbitrary_types_allowed=True):
    """**AbstractBaseClass** for Orbiter objects, provides a number of properties

    :param imports: List of OrbiterRequirement objects
    :type imports: List[OrbiterRequirement]
    :param orbiter_kwargs: Optional dictionary of keyword arguments, to preserve what was originally parsed by a rule
    :type orbiter_kwargs: dict, optional
    :param orbiter_conns: Optional set of OrbiterConnection objects
    :type orbiter_conns: Set[OrbiterConnection], optional
    :param orbiter_vars: Optional set of OrbiterVariable objects
    :type orbiter_vars: Set[OrbiterVariable], optional
    :param orbiter_env_vars: Optional set of OrbiterEnvVar objects
    :type orbiter_env_vars: Set[OrbiterEnvVar], optional
    :param orbiter_includes: Optional set of OrbiterInclude objects
    :type orbiter_includes: Set[OrbiterInclude], optional
    """

    imports: ImportList
    orbiter_kwargs: dict = None

    orbiter_conns: Set[OrbiterConnection] | None = None
    orbiter_vars: Set[OrbiterVariable] | None = None
    orbiter_env_vars: Set[OrbiterEnvVar] | None = None
    orbiter_includes: Set[OrbiterInclude] | None = None


def conn_id(conn_id: str, prefix: str = "", conn_type: str = "generic") -> dict:
    """Helper function to add an [OrbiterConnection][orbiter.objects.connection.OrbiterConnection]
    when adding a `conn_id`

    Usage:
    ```python
    OrbiterBashOperator(
        **conn_id("my_conn_id")
    )
    ```
    :param conn_id: The connection id
    :type conn_id: str
    :param prefix: Prefix to add to the connection id
    :type prefix: str, optional
    :param conn_type: Connection type
    :type conn_type: str, optional
    :return: Dictionary to unpack (e.g. `**conn_id(...)`)
    :rtype: dict
    """
    from orbiter.objects.connection import OrbiterConnection

    return {
        f"{prefix + '_' if prefix else ''}conn_id": conn_id,
        "orbiter_conns": {
            OrbiterConnection(
                conn_id=conn_id, **({"conn_type": conn_type} if conn_type else {})
            )
        },
    }


def pool(name: str) -> dict:
    """Helper function to add a [OrbiterPool][orbiter.objects.pool.OrbiterPool] when adding a `pool`

    Usage:
    ```python
    OrbiterBashOperator(
        **pool("my_pool")
    )
    ```
    :param name: The pool name
    :type name: str
    :return: Dictionary to unpack (e.g. `**pool(...)`)
    :rtype: dict
    """
    from orbiter.objects.pool import OrbiterPool

    return {
        "pool": name,
        "orbiter_pools": {OrbiterPool(name=name)},
    }

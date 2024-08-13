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
    """AbstractBaseClass for Orbiter objects

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


def conn_id(val: str, prefix: str = "", conn_type: str = "generic"):
    """Helper function to add an [OrbiterConnection][orbiter.objects.connection.OrbiterConnection]
    when adding a `conn_id`"""
    from orbiter.objects.connection import OrbiterConnection

    return {
        f"{prefix + '_' if prefix else ''}conn_id": val,
        "orbiter_conns": {
            OrbiterConnection(
                conn_id=val, **({"conn_type": conn_type} if conn_type else {})
            )
        },
    }

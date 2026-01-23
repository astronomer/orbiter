from __future__ import annotations

from abc import ABC
from typing import List, Set, ClassVar, Annotated, Iterable

from pydantic import BaseModel, AfterValidator, validate_call

from orbiter.meta import OrbiterMeta
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.connection import OrbiterConnection
from orbiter.objects.env_var import OrbiterEnvVar
from orbiter.objects.variable import OrbiterVariable
from orbiter.objects.include import OrbiterInclude


CALLBACK_KEYS = [
    "on_success_callback",
    "on_failure_callback",
    "sla_miss_callback",
    "on_retry_callback",
    "on_execute_callback",
    "on_skipped_callback",
]

RenderAttributes = ClassVar[list[str]]


def validate_imports(v):
    assert len(v)
    for i in v:
        assert isinstance(i, OrbiterRequirement)
    return v


ImportList = Annotated[List[OrbiterRequirement], AfterValidator(validate_imports)]


class OrbiterBase(BaseModel, ABC, arbitrary_types_allowed=True):
    """**AbstractBaseClass** for Orbiter objects, provides a number of properties

    :param imports: List of [OrbiterRequirement][orbiter.objects.requirement.OrbiterRequirement] objects
    :type imports: List[OrbiterRequirement]
    :param orbiter_kwargs: Optional dictionary of keyword arguments, to preserve what was originally parsed by a rule
    :type orbiter_kwargs: dict, optional
    :param orbiter_conns: Optional set of [OrbiterConnection][orbiter.objects.connection.OrbiterConnection] objects
    :type orbiter_conns: Set[OrbiterConnection], optional
    :param orbiter_env_vars: Optional set of [OrbiterEnvVar][orbiter.objects.env_var.OrbiterEnvVar] objects
    :type orbiter_env_vars: Set[OrbiterEnvVar], optional
    :param orbiter_includes: Optional set of [OrbiterInclude][orbiter.objects.include.OrbiterInclude] objects
    :type orbiter_includes: Set[OrbiterInclude], optional
    :param orbiter_vars: Optional set of [OrbiterVariable][orbiter.objects.variable.OrbiterVariable] objects
    :type orbiter_vars: Set[OrbiterVariable], optional
    """

    imports: ImportList
    orbiter_kwargs: dict | None = None
    orbiter_meta: list[OrbiterMeta] | OrbiterMeta | None = None

    orbiter_conns: Set[OrbiterConnection] | None = None
    orbiter_env_vars: Set[OrbiterEnvVar] | None = None
    orbiter_includes: Set[OrbiterInclude] | None = None
    orbiter_vars: Set[OrbiterVariable] | None = None

    def add_requirements(self, requirements: OrbiterRequirement | Iterable[OrbiterRequirement]) -> "OrbiterBase":
        """Add [OrbiterRequirements][orbiter.objects.requirement.OrbiterRequirement] to imports

        ```pycon
        >>> from orbiter.objects.operators.bash import OrbiterBashOperator
        >>> OrbiterBashOperator(
        ...     task_id='foo', bash_command='echo hello'
        ... ).add_requirements(
        ...     OrbiterRequirement(package='apache-airflow', names=['DAG'], module='airflow')
        ... ).imports
        [OrbiterRequirement(names=[BashOperator], package=apache-airflow, module=airflow.operators.bash, sys_package=None), OrbiterRequirement(names=[DAG], package=apache-airflow, module=airflow, sys_package=None)]

        >>> OrbiterBashOperator(
        ...     task_id='foo', bash_command='echo hello'
        ... ).add_requirements([
        ...     OrbiterRequirement(package='pandas', names=['DataFrame'], module='pandas'),
        ...     OrbiterRequirement(package='numpy', names=['array'], module='numpy')
        ... ]).imports  # doctest: +ELLIPSIS
        [OrbiterRequirement(names=[BashOperator], package=apache-airflow, module=airflow.operators.bash, sys_package=None), OrbiterRequirement(names=[DataFrame], package=pandas, module=pandas, sys_package=None), OrbiterRequirement(names=[array], package=numpy, module=numpy, sys_package=None)]

        ```

        :param requirements: Single or list of [OrbiterRequirement][orbiter.objects.requirement.OrbiterRequirement]
        :type requirements: OrbiterRequirement | Iterable[OrbiterRequirement]
        :return: self
        :rtype: OrbiterBase
        """
        for requirement in [requirements] if isinstance(requirements, OrbiterRequirement) else requirements:
            self.imports.append(requirement)
        return self

    def add_connections(self, connections: OrbiterConnection | Iterable[OrbiterConnection]) -> "OrbiterBase":
        """Add [OrbiterConnections][orbiter.objects.connection.OrbiterConnection] to orbiter_conns

        ```pycon
        >>> from orbiter.objects.operators.bash import OrbiterBashOperator
        >>> OrbiterBashOperator(
        ...     task_id='foo', bash_command='echo hello'
        ... ).add_connections(
        ...     OrbiterConnection(conn_id='postgres')
        ... ).orbiter_conns
        {OrbiterConnection(conn_id=postgres, conn_type=generic)}

        >>> OrbiterBashOperator(
        ...     task_id='foo', bash_command='echo hello'
        ... ).add_connections([
        ...     OrbiterConnection(conn_id='postgres'),
        ...     OrbiterConnection(conn_id='mysql')
        ... ]).orbiter_conns  # doctest: +SKIP
        {OrbiterConnection(conn_id=postgres, conn_type=generic), OrbiterConnection(conn_id=mysql, conn_type=generic)}

        ```

        :param connections: Single or iterable of [OrbiterConnection][orbiter.objects.connection.OrbiterConnection]
        :type connections: OrbiterConnection | Iterable[OrbiterConnection]
        :return: self
        :rtype: OrbiterBase
        """
        if self.orbiter_conns is None:
            self.orbiter_conns = set()
        for connection in [connections] if isinstance(connections, OrbiterConnection) else connections:
            self.orbiter_conns.add(connection)
        return self

    def add_env_vars(self, env_vars: OrbiterEnvVar | Iterable[OrbiterEnvVar]) -> "OrbiterBase":
        """Add [OrbiterEnvVars][orbiter.objects.env_var.OrbiterEnvVar] to orbiter_env_vars

        ```pycon
        >>> from orbiter.objects.operators.bash import OrbiterBashOperator
        >>> OrbiterBashOperator(
        ...     task_id='foo', bash_command='echo hello'
        ... ).add_env_vars(
        ...     OrbiterEnvVar(key='ENV', value='prod')
        ... ).orbiter_env_vars
        {OrbiterEnvVar(key='ENV', value='prod')}

        >>> OrbiterBashOperator(
        ...     task_id='foo', bash_command='echo hello'
        ... ).add_env_vars([
        ...     OrbiterEnvVar(key='ENV', value='prod'),
        ...     OrbiterEnvVar(key='REGION', value='us-west-2')
        ... ]).orbiter_env_vars  # doctest: +SKIP
        {OrbiterEnvVar(key='ENV', value='prod'), OrbiterEnvVar(key='REGION', value='us-west-2')}

        ```

        :param env_vars: Single or iterable of [OrbiterEnvVar][orbiter.objects.env_var.OrbiterEnvVar]
        :type env_vars: OrbiterEnvVar | Iterable[OrbiterEnvVar]
        :return: self
        :rtype: OrbiterBase
        """
        if self.orbiter_env_vars is None:
            self.orbiter_env_vars = set()
        for env_var in [env_vars] if isinstance(env_vars, OrbiterEnvVar) else env_vars:
            self.orbiter_env_vars.add(env_var)
        return self

    def add_includes(self, includes: OrbiterInclude | Iterable[OrbiterInclude]) -> "OrbiterBase":
        """Add [OrbiterIncludes][orbiter.objects.include.OrbiterInclude] to orbiter_includes

        ```pycon
        >>> from orbiter.objects.operators.bash import OrbiterBashOperator
        >>> OrbiterBashOperator(
        ...     task_id='foo', bash_command='echo hello'
        ... ).add_includes(
        ...     OrbiterInclude(filepath='utils.py', contents='# Utils')
        ... ).orbiter_includes
        {OrbiterInclude(filepath='utils.py', contents='# Utils')}

        >>> OrbiterBashOperator(
        ...     task_id='foo', bash_command='echo hello'
        ... ).add_includes([
        ...     OrbiterInclude(filepath='utils.py', contents='# Utils'),
        ...     OrbiterInclude(filepath='helpers.py', contents='# Helpers')
        ... ]).orbiter_includes  # doctest: +SKIP
        {OrbiterInclude(filepath='utils.py', contents='# Utils'), OrbiterInclude(filepath='helpers.py', contents='# Helpers')}

        ```

        :param includes: Single or iterable of [OrbiterInclude][orbiter.objects.include.OrbiterInclude]
        :type includes: OrbiterInclude | Iterable[OrbiterInclude]
        :return: self
        :rtype: OrbiterBase
        """
        if self.orbiter_includes is None:
            self.orbiter_includes = set()
        for include in [includes] if isinstance(includes, OrbiterInclude) else includes:
            self.orbiter_includes.add(include)
        return self


def conn_id(conn_id: str, prefix: str = "", conn_type: str = "generic") -> dict:
    """Helper function to add an [OrbiterConnection][orbiter.objects.connection.OrbiterConnection]
    when adding a `conn_id`

    Usage:
    ```python
    OrbiterBashOperator(**conn_id("my_conn_id"))
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
        "orbiter_conns": {OrbiterConnection(conn_id=conn_id, **({"conn_type": conn_type} if conn_type else {}))},
    }


def pools(name: str, slots: int | None = None, pool_kwargs: dict[str, str | int] | None = None) -> dict:
    """Helper function to add a [OrbiterPool][orbiter.objects.pool.OrbiterPool] when adding a `pool`

    Usage:
    ```python
    >>> from orbiter.objects.operators.bash import OrbiterBashOperator
    >>> OrbiterBashOperator(
    ...     task_id="foo", bash_command="bar", **pools(name="my_pool", slots=1, pool_kwargs={"slots": 1})
    ... ) # doctest: +NORMALIZE_WHITESPACE
    foo_task = BashOperator(task_id='foo', pool='my_pool', pool_slots=1, bash_command='bar')

    ```
    :param name: The pool name
    :type name: str
    :param slots: The number of slots in the pool. Defaults to 128
    :type slots: int, optional
    :param pool_kwargs: Optional dictionary of keyword arguments for the OrbiterPool
    :type pool_kwargs: dict[str, str | int], optional
    :return: Dictionary to unpack (e.g. `**pools(...)`)
    :rtype: dict
    """
    from orbiter.objects.pool import OrbiterPool

    if pool_kwargs is None:
        pool_kwargs = {}

    return {
        "pool": name,
        **({"pool_slots": slots} if slots else {}),
        "orbiter_pool": OrbiterPool(name=name, **pool_kwargs),
    }


# https://github.com/pydantic/pydantic/issues/8790
OrbiterBase.add_requirements = validate_call()(OrbiterBase.add_requirements)
OrbiterBase.add_connections = validate_call()(OrbiterBase.add_connections)
OrbiterBase.add_env_vars = validate_call()(OrbiterBase.add_env_vars)
OrbiterBase.add_includes = validate_call()(OrbiterBase.add_includes)

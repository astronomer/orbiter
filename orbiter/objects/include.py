from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from orbiter.objects.requirement import OrbiterRequirement


class OrbiterInclude(BaseModel, extra="forbid"):
    """Represents an included file in the project directory, likely in the `/include` directory.

    e.g. to place a utility in `includes/util.py`:
    ```pycon
    >>> OrbiterInclude(
    ...   filepath="includes/util.py",
    ...   contents="def foo(): return 1",
    ... ) # doctest: +ELLIPSIS
    OrbiterInclude(filepath='includes/util.py', contents='...')

    ```
    If you then wanted to use that in a PythonOperator, in a Dag, you could use it like:
    ```pycon
    >>> from orbiter.objects.dag import OrbiterDAG
    >>> from orbiter.objects.operators.python import OrbiterPythonOperator
    >>> from orbiter.objects.requirement import OrbiterRequirement
    >>> OrbiterDAG(dag_id="foo", file_path="foo.py").add_tasks(OrbiterPythonOperator(
    ...     task_id="foo",
    ...     # Add the "include":
    ...     orbiter_includes={OrbiterInclude(filepath="includes/util.py", contents="def foo(): return 1")},
    ...     # Ensure it's imported in the DAG
    ...     imports=[OrbiterRequirement(module="includes.util", names=["foo"])],
    ...     python_callable="foo"
    ... ))
    from airflow import DAG
    from includes.util import foo
    with DAG(dag_id='foo'):
        foo_task = PythonOperator(task_id='foo', python_callable=foo)

    ```
    :param filepath: The relative path (from the output directory) to write the file to
    :type filepath: str
    :param contents: The contents of the file
    :type contents: str
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    filepath: str
    contents: str
    --8<-- [end:mermaid-props]
    """

    filepath: str
    contents: str

    @staticmethod
    def get_include_and_requirement(
        include_module_qualname: str,
        import_names: list[str],
        include_filepath: str | None = None,
        import_package: str | None = None,
        import_sys_package: str | None = None,
    ) -> "tuple[OrbiterInclude, OrbiterRequirement]":
        """Create an [OrbiterInclude][orbiter.objects.include.OrbiterInclude]
        and [OrbiterRequirement][orbiter.objects.requirement.OrbiterRequirement] from a qualified name
        to a python module within the translation.

        This allows you to store python that will be included into the Airflow Project as a normal python file,
        and inject it for inclusion by looking up the relative path via the import system.

        ```pycon
        >>> OrbiterInclude.get_include_and_requirement(
        ...   include_module_qualname="orbiter.objects.include",
        ...   import_names=["OrbiterInclude"]
        ... ) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        (OrbiterInclude(filepath='include/include.py', contents='...'),
            OrbiterRequirement(names=[OrbiterInclude], package=None, module=include.include, sys_package=None))

        ```
        :param include_module_qualname: The qualified name of the module to include, e.g. `orbiter.objects.include` for this module.
        :type include_module_qualname: str
        :param import_names: The names to import from the module, e.g. `["OrbiterInclude"]` for this class.
        :type import_names: list[str]
        :param include_filepath: Optional filepath to write the include to creating the file as `include/???.py`
        :type include_filepath: str, optional
        :param import_package: Optional Python package, to add to the project if specified.
        :type import_package: str, optional
        :param import_sys_package: Optional Debian package, to add to the project if specified.
        :type import_sys_package: str, optional
        :return: A tuple of the [OrbiterInclude][orbiter.objects.include.OrbiterInclude] and [OrbiterRequirement][orbiter.objects.requirement.OrbiterRequirement]
        """
        from orbiter.objects.requirement import OrbiterRequirement
        from importlib.util import find_spec
        from pathlib import Path

        default_include_filepath = "include"
        default_include_file_extension = "py"

        file_name = include_module_qualname.rsplit(".", maxsplit=1)[-1]
        if not include_filepath:
            include_filepath = f"{default_include_filepath}/{file_name}.{default_include_file_extension}"
            file_extension = default_include_file_extension
        else:
            file_extension = include_filepath.rsplit(".", maxsplit=1)[-1]

        spec = find_spec(include_module_qualname)
        if spec is None or spec.origin is None:
            raise ImportError(
                f"Cannot find module specification for '{include_module_qualname}'. "
                "Ensure the module is importable and available on PYTHONPATH."
            )

        return OrbiterInclude(
            filepath=include_filepath,
            contents=Path(spec.origin).read_text(),
        ), OrbiterRequirement(
            module=include_filepath.replace("/", ".").replace("." + file_extension, ""),
            names=import_names,
            package=import_package,
            sys_package=import_sys_package,
        )

    def __hash__(self):
        return hash(self.contents + self.filepath)

    def render(self):
        return self.contents

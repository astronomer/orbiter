from pydantic import BaseModel


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

    def __hash__(self):
        return hash(self.contents + self.filepath)

    def render(self):
        return self.contents

from __future__ import annotations
import ast
import inspect
from abc import ABC, abstractmethod
from typing import List, Callable


def py_bitshift(left: str | List[str], right: str | List[str], is_downstream: bool = True):
    """
    >>> render_ast(py_bitshift("foo", "bar", is_downstream=False))
    'foo << bar'
    >>> render_ast(py_bitshift("foo", "bar"))
    'foo >> bar'
    >>> render_ast(py_bitshift("foo", ["bar", "baz"]))
    'foo >> [bar, baz]'
    >>> render_ast(py_bitshift(["foo", "bar"], "baz"))
    '[foo, bar] >> baz'
    """
    left = ast.Name(id=left) if isinstance(left, str) else ast.List(elts=[ast.Name(id=elt) for elt in left])
    right = ast.Name(id=right) if isinstance(right, str) else ast.List(elts=[ast.Name(id=elt) for elt in right])
    return ast.Expr(value=ast.BinOp(left=left, op=ast.RShift() if is_downstream else ast.LShift(), right=right))


def py_assigned_object(ast_name: str, obj: str, **kwargs) -> ast.Assign:
    """
    >>> render_ast(py_assigned_object("foo", "Bar", baz="bop"))
    "foo = Bar(baz='bop')"
    """
    return ast.Assign(
        lineno=0,
        targets=[ast.Name(id=ast_name)],
        value=ast.Call(
            func=ast.Name(id=obj),
            args=[],
            keywords=[
                ast.keyword(
                    arg=arg,
                    value=(ast.Constant(value=value) if not isinstance(value, ast.AST) else value),
                )
                for arg, value in kwargs.items()
            ],
        ),
    )


def py_object(name: str, *args, **kwargs) -> ast.Expr:
    """
    >>> render_ast(py_object("Bar", "boom", "bing", baz="bop"))
    "Bar('boom', 'bing', baz='bop')"
    """
    return ast.Expr(
        value=ast.Call(
            func=ast.Name(id=name),
            args=[ast.Constant(value=value) for value in args],
            keywords=[ast.keyword(arg=arg, value=ast.Constant(value=value)) for arg, value in kwargs.items()],
        )
    )


def py_root(*args) -> ast.Module:
    """
    :param args: ast objects, such as ast.Expr
    :return: root ast.Module, which can be `ast.unparse`d
    """
    # noinspection PyTypeChecker
    return ast.Module(body=args, type_ignores=[])


def py_import(names: List[str], module: str | None = None) -> ast.ImportFrom | ast.Import | list:
    """
    :param module: e.g. `airflow.operators.bash` for `from airflow.operators.bash import BashOperator`
    :param names: e.g. `BashOperator` for `from airflow.operators.bash import BashOperator`
    :return: ast.ImportFrom
    >>> render_ast(py_import(module="airflow.operators.bash", names=["BashOperator"]))
    'from airflow.operators.bash import BashOperator'

    >>> render_ast(py_import(names=["json"]))
    'import json'
    """
    if module is not None:
        return ast.ImportFrom(module=module, names=[ast.alias(name=name) for name in names], level=0)
    elif module is None and names:
        return ast.Import(names=[ast.alias(name=name) for name in names], level=0)
    else:
        return []


def py_with(item: ast.expr, body: List[ast.stmt], assignment: str | None = None) -> ast.With:
    # noinspection PyTypeChecker
    """
    >>> render_ast(py_with(py_object("Bar"), [ast.Pass()]))
    'with Bar():\\n    pass'
    >>> render_ast(
    ...     py_with(
    ...         py_object("DAG", dag_id="foo").value,
    ...         [py_object("Operator", task_id="foo")],
    ...     )
    ... )
    "with DAG(dag_id='foo'):\\n    Operator(task_id='foo')"
    >>> render_ast(
    ...     py_with(
    ...         py_object("DAG", dag_id="foo").value,
    ...         [py_object("Operator", task_id="foo")],
    ...         "dag",
    ...     )
    ... )
    "with DAG(dag_id='foo') as dag:\\n    Operator(task_id='foo')"
    """
    if isinstance(item, ast.Expr):
        item = item.value
    # noinspection PyTypeChecker
    return ast.With(
        items=[
            ast.withitem(
                context_expr=item,
                optional_vars=[ast.Name(id=assignment)] if assignment else [],
            )
        ],
        body=body,
        lineno=1,
    )


def py_function(
    c: Callable | str, decorator_names: List[str] | None = None, decorator_kwargs: List[dict] | None = None
):
    """
    ```pycon
    >>> def foo(a, b):
    ...     print(a + b)
    >>> render_ast(py_function(foo))
    'def foo(a, b):\\n    print(a + b)'

    >>> render_ast(py_function(foo, decorator_names=["foo"], decorator_kwargs=[{"bar": "baz"}]))
    "@foo(bar='baz')\\ndef foo(a, b):\\n    print(a + b)"

    ```
    """
    fn = ast.parse(inspect.getsource(c) if isinstance(c, Callable) else c).body[0]
    if decorator_names and decorator_kwargs:
        fn.decorator_list = [
            ast.Call(
                func=ast.Name(id=decorator_name),
                args=[],
                keywords=[
                    ast.keyword(arg=arg, value=ast.Constant(value=value)) for arg, value in decorator_kwarg.items()
                ],
            )
            for decorator_name, decorator_kwarg in zip(decorator_names or [], decorator_kwargs or [])
        ]
    return fn


def py_reference(name: str) -> ast.Expr:
    """
    ```pycon
    >>> render_ast(py_reference("foo"))
    'foo'

    ```
    """
    return ast.Expr(value=ast.Name(id=name))


def render_ast(ast_object) -> str:
    if ast_object is None:
        return ""
    return ast.unparse(ast_object)


class OrbiterASTBase(ABC):
    @abstractmethod
    def _to_ast(self) -> ast.stmt | ast.Module:
        raise NotImplementedError()

    def __str__(self):
        return render_ast(self._to_ast())

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return str(self) == str(other)

    def __lt__(self, other):
        return str(self) < str(other)

    def __gt__(self, other):
        return str(self) > str(other)

    def __hash__(self):
        return hash(str(self))


if __name__ == "main":
    import doctest

    doctest.testmod()

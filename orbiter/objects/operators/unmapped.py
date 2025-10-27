from importlib.util import find_spec
from pathlib import Path
from typing import Literal, Set

from orbiter.objects import ImportList, RenderAttributes, OrbiterInclude
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.task import OrbiterOperator


class OrbiterUnmappedOperator(OrbiterOperator):
    """
    An Unmapped Operator, to mark when orbiter was unable to translate input.
    Inherits an EmptyOperator, which does nothing.

    ```pycon
    >>> OrbiterUnmappedOperator(task_id="foo", source='{"some_key": "some_value"}')
    foo_task = UnmappedOperator(task_id='foo', source='{"some_key": "some_value"}')

    ```
    :param task_id: The `task_id` for the operator
    :type task_id: str
    :param source: The original source input that was unable to be mapped
    :type source: str
    :param **kwargs: Extra arguments to pass to the operator
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "UnmappedOperator"
    task_id: str
    source: str
    --8<-- [end:mermaid-props]
    """
    orbiter_type: Literal["OrbiterUnmappedOperator"] = "OrbiterUnmappedOperator"

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow",
            module="include.unmapped",
            names=["UnmappedOperator"],
        )
    ]
    orbiter_includes: Set["OrbiterInclude"] = {
        OrbiterInclude(
            filepath="include/unmapped.py",
            contents=Path(find_spec("orbiter.assets.operators.unmapped_src").origin).read_text(),
        ),
    }

    operator: str = "UnmappedOperator"

    source: str

    render_attributes: RenderAttributes = OrbiterOperator.render_attributes + ["source"]

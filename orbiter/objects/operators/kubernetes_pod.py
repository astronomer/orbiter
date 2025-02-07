from __future__ import annotations

from typing import Set, TYPE_CHECKING, Union, Any

from orbiter.objects import ImportList, OrbiterRequirement, OrbiterConnection
from orbiter.objects.task import OrbiterOperator, RenderAttributes

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from kubernetes.client.models import V1EnvVar  # noqa: F401

    # noinspection PyUnresolvedReferences
    from kubernetes.client.models import V1ResourceRequirements  # noqa: F401

    # noinspection PyUnresolvedReferences
    from kubernetes.client.models import V1LocalObjectReference  # noqa: F401


class OrbiterKubernetesPodOperator(OrbiterOperator):
    """
    An Airflow
    [KubernetesPodOperator](https://registry.astronomer.io/providers/apache-airflow-providers-cncf-kubernetes/versions/latest/modules/KubernetesPodOperator).
    Used to launch a Docker container in a Kubernetes cluster.

    ```pycon
    >>> OrbiterKubernetesPodOperator(
    ...     task_id="foo",
    ...     kubernetes_conn_id="KUBERNETES",
    ...     image="my-docker-image"
    ... )
    foo_task = KubernetesPodOperator(task_id='foo', kubernetes_conn_id='KUBERNETES', image='my-docker-image')

    ```
    :param task_id: The `task_id` for the operator
    :type task_id: str
    :param kubernetes_conn_id: The Kubernetes connection to use. Defaults to "KUBERNETES"
    :type kubernetes_conn_id: str, optional
    :param image: The Docker image to launch
    :type image: str
    :param cmds: The commands to run in the container, defaults container Entrypoint
    :type cmds: list[str] | None, optional
    :param arguments: The arguments to pass to the commands, defaults container commands
    :type arguments: list[str] | None, optional
    :param env_vars: The environment variables to set in the container, defaults to None
    :type env_vars: dict[str, str] | list[V1EnvVar] | None, optional
    :param container_resources: The resource requirements for the container, defaults to None
    :type container_resources: V1ResourceRequirements | None, optional
    :param image_pull_secrets: The secrets to use for pulling the Docker image, defaults to None
    :type image_pull_secrets: list[V1LocalObjectReference] | None, optional
    :param **kwargs: Extra arguments to pass to the operator
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "KubernetesPodOperator"
    task_id: str
    kubernetes_conn_id: str
    image: str
    cmds: list | None
    arguments: list | None
    env_vars: dict | None
    container_resources: V1ResourceRequirements | None
    image_pull_secrets: list | None
    --8<-- [end:mermaid-props]
    """

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow-providers-cncf-kubernetes",
            module="airflow.providers.cncf.kubernetes.operators.pod",
            names=["KubernetesPodOperator"],
        )
    ]
    operator: str = "KubernetesPodOperator"

    # noinspection Pydantic
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes + [
        "kubernetes_conn_id",
        "image",
        "cmds",
        "arguments",
        "env_vars",
        "container_resources",
        "image_pull_secrets",
    ]
    kubernetes_conn_id: str | None = "KUBERNETES"
    image: str
    cmds: list[str] | None = None
    arguments: list[str] | None = None
    env_vars: dict[str, str] | list[Any] | None = None  # V1EnvVar
    container_resources: Union[Any, None] = None  # V1ResourceRequirements
    image_pull_secrets: list[Any] | None = None  # V1LocalObjectReference
    orbiter_conns: Set[OrbiterConnection] | None = {OrbiterConnection(conn_id="KUBERNETES", conn_type="kubernetes")}


if __name__ == "__main__":
    import doctest

    doctest.testmod()

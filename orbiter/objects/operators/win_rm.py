from orbiter.objects import ImportList, OrbiterRequirement
from orbiter.objects.task import OrbiterOperator, RenderAttributes


class OrbiterWinRMOperator(OrbiterOperator):
    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow-providers-microsoft-winrm",
            module="airflow.providers.microsoft.winrm.operators.winrm",
            names=["WinRMOperator"],
        )
    ]
    operator: str = "WinRMOperator"
    # noinspection Pydantic
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes + [
        "ssh_conn_id",
        "command",
    ]
    ssh_conn_id: str
    command: str

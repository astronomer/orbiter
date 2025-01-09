from orbiter.objects import ImportList, OrbiterRequirement
from orbiter.objects.task import OrbiterOperator, RenderAttributes


class OrbiterLivyOperator(OrbiterOperator):
    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow-providers-apache-livy",
            module="airflow.providers.apache.livy.operators.livy",
            names=["LivyOperator"],
        )
    ]
    operator: str = "LivyOperator"

    # noinspection Pydantic
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes

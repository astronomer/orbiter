from orbiter_community_translations.dag_factory import translation_ruleset  # (1)!

from orbiter.objects.operators.ssh import OrbiterSSHOperator  # (2)!
from orbiter.rules import task_rule  # (3)!


@task_rule(priority=99)  # (4)!
def ssh_rule(val: dict):
    """Demonstration of overriding rulesets, by switching DAG Factory BashOperators to SSHOperators"""
    if val.pop("operator", "") == "BashOperator":  # (5)!
        return OrbiterSSHOperator(  # (6)!
            command=val.pop("bash_command"),
            doc="Hello World!",
            **{k: v for k, v in val if k != "dependencies"},
        )
    else:
        return None


translation_ruleset.task_ruleset.ruleset.append(ssh_rule)  # (7)!

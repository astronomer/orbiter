from __future__ import annotations

from orbiter_community_translations.dag_factory import translation_ruleset

from orbiter.objects.operators.ssh import OrbiterSSHOperator
from orbiter.objects.task import OrbiterOperator
from orbiter.rules import task_rule


# A higher priority means it will be applied before other rules
@task_rule(priority=99)
def ssh_rule(val: dict) -> OrbiterOperator | None:
    """Demonstration of overriding rulesets, by switching DAG Factory BashOperators to SSHOperators"""
    if val.pop("operator", "") == "BashOperator":
        del val["dependencies"]
        return OrbiterSSHOperator(
            command=val.pop("bash_command"),
            doc="Hello World!",
            **val,
        )
    else:
        return None


#  add a custom rule to the default ruleset
translation_ruleset.task_ruleset.ruleset.append(ssh_rule)

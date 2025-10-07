from orbiter.objects.callbacks.smtp import OrbiterSmtpNotifierCallback
from orbiter.objects.operators.bash import OrbiterBashOperator


def test_task_callback_parse():
    test = OrbiterBashOperator(
        task_id="task_id",
        bash_command="echo 'hello world'",
        on_failure_callback=OrbiterSmtpNotifierCallback(
            to="foo@test.com", from_email="bar@test.com", subject="Hello", html_content="World"
        ),
    )
    actual_json = test.model_dump_json()
    actual = OrbiterBashOperator.model_validate_json(actual_json)
    assert actual == test

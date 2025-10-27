import pytest

from orbiter.objects.operators.python import OrbiterPythonOperator


def printer(x):
    print(x)


def test_python_op_parse():
    test = OrbiterPythonOperator(
        task_id="task_id",
        python_callable=printer,
    )
    actual_json = test.model_dump_json()
    actual = OrbiterPythonOperator.model_validate_json(actual_json)
    assert actual == test


@pytest.mark.xfail(
    reason="""
TODO - Error:
E     Value error, <object object at 0x10a384320> is not a valid Sentinel
[type=value_error, input_value='gASVqhQAAAAAAACMF3JpY2hf...saWFzZXOUXZRoXmgRdWIu\n', input_type=str]
"""
)
def test_python_op_parse_import():
    from orbiter.__main__ import document

    test = OrbiterPythonOperator(
        task_id="task_id",
        python_callable=document,
    )
    actual_json = test.model_dump_json()
    actual = OrbiterPythonOperator.model_validate_json(actual_json)
    assert actual == test


@pytest.mark.xfail(reason="Ast Helper Not expecting builtin - TODO")
def test_python_op_parse_builtin():
    # Error in ast_helper stuff, not expecting builtin
    test = OrbiterPythonOperator(
        task_id="task_id",
        python_callable=print,
    )
    actual_json = test.model_dump_json()
    actual = OrbiterPythonOperator.model_validate_json(actual_json)
    assert actual == test

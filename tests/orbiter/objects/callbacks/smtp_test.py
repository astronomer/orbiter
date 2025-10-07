from orbiter.objects.callbacks.smtp import OrbiterSmtpNotifierCallback


def test_callback_parse_callback():
    test = OrbiterSmtpNotifierCallback(
        to="foo@test.com", from_email="bar@test.com", subject="Hello", html_content="World"
    )
    actual_json = test.model_dump_json()
    actual = OrbiterSmtpNotifierCallback.model_validate_json(actual_json)
    assert actual == test

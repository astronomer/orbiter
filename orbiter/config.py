import os
import sys

TRANSLATION_VERSION = os.getenv("ORBITER_TRANSLATION_VERSION", "latest")
"""The version of the translation ruleset to download. This can be overridden."""

ORBITER_TASK_SUFFIX = os.getenv("ORBITER_TASK_SUFFIX", "_task")
"""By default, we add `_task` as a suffix to a task name to prevent name collision issues. This can be overridden."""

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
"""You can set the log level to DEBUG to see more detailed logs."""

KG_ACCOUNT_ID = "3b189b4c-c047-4fdb-9b46-408aa2978330"
RUNNING_AS_BINARY = getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")

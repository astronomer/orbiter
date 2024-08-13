from __future__ import annotations

from abc import ABC
from typing import ClassVar


class AirflowSettingsRender(ABC):
    """Used to render objects as `yaml` to `airflow_settings.yaml` file"""

    render_prefix: ClassVar[str]

    def render(self) -> dict:
        # noinspection PyUnresolvedReferences
        return {
            (self.render_prefix if not k.startswith(self.render_prefix) else "") + k: v
            for k, v in self.model_dump().items()
        }

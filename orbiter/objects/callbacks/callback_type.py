from __future__ import annotations

from typing import Annotated

from pydantic import Field

from orbiter.objects.callbacks import OrbiterCallback
from orbiter.objects.callbacks.smtp import OrbiterSmtpNotifierCallback

CallbackType = Annotated[OrbiterSmtpNotifierCallback | OrbiterCallback | None, Field(default=None)]

from __future__ import annotations

from typing import Annotated, Optional

from pydantic import Field

from orbiter.objects.callbacks import OrbiterCallback
from orbiter.objects.callbacks.smtp import OrbiterSmtpNotifierCallback

CallbackType = Annotated[Optional[OrbiterSmtpNotifierCallback | OrbiterCallback], Field(default=None)]

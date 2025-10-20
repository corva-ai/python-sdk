import contextlib
import logging
import sys
import time
from contextlib import suppress
from typing import Optional

from corva.configuration import SETTINGS

logging.Formatter.converter = time.gmtime  # log time as UTC

CORVA_LOGGER = logging.getLogger('corva')
CORVA_LOGGER.setLevel(SETTINGS.LOG_LEVEL)

logging.getLogger("urllib3.connectionpool").setLevel(SETTINGS.LOG_LEVEL)

# Disable propagation to avoid duplicate CloudWatch logs from AWS Lambda's
# root handler. We will explicitly attach any OTel log handlers directly
# to CORVA_LOGGER within LoggingContext when log sending is enabled.
# see https://github.com/corva-ai/otel/pull/37
# see https://corvaqa.atlassian.net/browse/EE-31
CORVA_LOGGER.propagate = False


def _is_otel_handler(handler: logging.Handler) -> bool:
    """Best-effort detection of OTel log sending handlers.

    We match by class/module name to avoid importing OTel directly.
    """
    try:
        module = getattr(handler.__class__, "__module__", "") or ""
        name = handler.__class__.__name__
        ident = f"{module}.{name}".lower()
        return ("otel" in ident) or ("opentelemetry" in ident)
    except Exception:
        return False


def _gather_otel_handlers_from_root() -> list[logging.Handler]:
    """Collect OTel handlers already attached to the root logger.

    We reuse existing handler instances and attach them to CORVA_LOGGER
    to keep OTel log sending while propagation is disabled.
    """
    root = logging.getLogger()
    handlers = getattr(root, "handlers", []) or []
    return [h for h in handlers if _is_otel_handler(h)]


def get_formatter(
    aws_request_id: bool, asset_id: bool, app_connection_id: bool
) -> logging.Formatter:
    return logging.Formatter(
        f'%(asctime)s.%(msecs)03dZ '
        f'{"%(aws_request_id)s " if aws_request_id else ""}'
        f'%(levelname)s '
        f'{"ASSET=%(asset_id)s " if asset_id else ""}'
        f'{"AC=%(app_connection_id)s " if app_connection_id else ""}'
        f'| %(message)s',
        '%Y-%m-%dT%H:%M:%S',
    )


class CorvaLoggerFilter(logging.Filter):
    """Injects fields into logging.LogRecord instance for usage in logging.Formatter."""

    def __init__(
        self,
        aws_request_id: str,
        asset_id: Optional[int],
        app_connection_id: Optional[int],
    ):
        logging.Filter.__init__(self)

        self.aws_request_id = aws_request_id
        self.asset_id = asset_id
        self.app_connection_id = app_connection_id

    def filter(self, record):
        record.aws_request_id = self.aws_request_id
        record.asset_id = self.asset_id
        record.app_connection_id = self.app_connection_id

        return True


class CorvaLoggerHandler(logging.Handler):
    """Logging handler with constraints.

    The handler logs to sys.stdout and has following functionality:
        1. Truncates the message to not exceed max message size.
        2. Disables the logging after reaching max message count and
            logs corresponding warning.

    Args:
        max_message_size: Maximum allowed message size in bytes.
            Messages that exceed this limit get truncated.
        max_message_count: Maximum allowed number of logged messages.
            After reaching this limit logging gets disabled.
        logger: Logger that is used to log the warning about reaching max
            message count.
        placeholder: String that will appear at the end of the message
            if it has been truncated.
    """

    TERMINATOR = '\n'

    def __init__(
        self,
        max_message_size: int,
        max_message_count: int,
        logger: logging.Logger,
        placeholder: str,
    ):
        logging.Handler.__init__(self)

        self.max_message_size = max_message_size
        self.max_message_count = max_message_count
        self.logger = logger
        self.placeholder = f'{placeholder}{self.TERMINATOR}'

        self.logging_warning = False
        # one extra message to log the warning
        self.residue_message_count = self.max_message_count + 1

    def emit(self, record: logging.LogRecord) -> None:
        if self.residue_message_count == 0:
            return

        if self.residue_message_count > 1 or self.logging_warning:
            self.log(message=self.format(record))
            self.residue_message_count -= 1

        if self.residue_message_count == 1:
            self.logging_warning = True
            self.logger.warning(
                f'Disabling the logging as maximum number of logged messages '
                f'was reached: {self.max_message_count}.'
            )

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)

        # https://github.com/debug-js/debug/issues/296#issuecomment-289595923
        # For CloudWatch `\n` means end of whole log and `\r` means end of line in
        # multiline logs.
        # Replace `\n` for `\r` for logs to display correctly in CloudWatch.
        message = message.replace('\n', '\r')
        message = f'{message}{self.TERMINATOR}'

        extra_chars_count = len(message) - self.max_message_size

        if extra_chars_count <= 0:
            # no need to truncate the message
            return message

        message_end_idx = len(message) - (extra_chars_count + len(self.placeholder))

        if message_end_idx <= 0:
            return ''

        message = message[:message_end_idx] + self.placeholder

        return message

    def log(self, message: str) -> None:
        sys.stdout.write(message)


class LoggingContext(contextlib.ContextDecorator):
    """Context manager to configure logger to use specified handlers.

    User handler does not get any modifications.

    Handler gets following modifications:
        - Added CorvaLoggerFilter to filters.
        - Set log level from settings.
        - Set Corva formatter.

    Context allows changing filter's fields dynamically. It will update logging
    formatter as needed.

    Logger gets its old handlers back at the end of the context.

    Attributes:
        filter: Logging filter that gets set in the handler.
        handler: Logging handler that gets modified and set in the logger.
        user_handler: Logging handler that gets set in the logger without modifications.
        logger: Logger to configure.
        old_handlers: Logger's own handlers before the update.
    """

    def __init__(
        self,
        aws_request_id: str,
        asset_id: Optional[int],
        app_connection_id: Optional[int],
        handler: logging.Handler,
        user_handler: Optional[logging.Handler],
        logger: logging.Logger,
    ):
        self.filter = CorvaLoggerFilter(
            aws_request_id=aws_request_id,
            asset_id=asset_id,
            app_connection_id=app_connection_id,
        )

        self.handler = handler
        self.handler.setLevel(SETTINGS.LOG_LEVEL)
        self.handler.addFilter(self.filter)
        self.set_formatter()

        self.user_handler = user_handler
        self.logger = logger

    @property
    def asset_id(self) -> Optional[int]:
        """Asset id used in the logging filter."""

        return self.filter.asset_id

    @asset_id.setter
    def asset_id(self, value: Optional[int]) -> None:
        self.filter.asset_id = value
        self.set_formatter()

    @property
    def app_connection_id(self) -> Optional[int]:
        """App connection id used in the logging filter."""

        return self.filter.app_connection_id

    @app_connection_id.setter
    def app_connection_id(self, value: Optional[int]) -> None:
        self.filter.app_connection_id = value
        self.set_formatter()

    def set_formatter(self):
        self.handler.setFormatter(
            get_formatter(
                aws_request_id=True,
                asset_id=self.asset_id is not None,
                app_connection_id=self.app_connection_id is not None,
            )
        )

    def __enter__(self):
        self.old_handlers = self.logger.handlers

        # Build the handler chain for CORVA_LOGGER.
        handlers = [self.handler]
        if self.user_handler:
            handlers.append(self.user_handler)

        # If OTel log sending is enabled and an OTel handler exists on root,
        # attach it to CORVA_LOGGER as well so propagation can remain disabled
        # (avoids AWS root duplication) while still exporting logs via OTel.

        if not SETTINGS.OTEL_LOG_SENDING_DISABLED:
            # Fail-safe: never break logging if detection fails
            with suppress(Exception):
                for handler in _gather_otel_handlers_from_root():
                    if handler not in handlers:
                        handlers.append(handler)

        self.logger.handlers = handlers

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.handlers = self.old_handlers

        return False  # exception will be propagated

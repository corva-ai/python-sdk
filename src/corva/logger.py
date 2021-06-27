import contextlib
import logging
import logging.config
import sys
import time
import traceback
from typing import Optional

from corva.configuration import SETTINGS

logging.Formatter.converter = time.gmtime  # log time as UTC

CORVA_LOGGER = logging.getLogger('corva')
CORVA_LOGGER.setLevel(SETTINGS.LOG_LEVEL)
CORVA_LOGGER.propagate = False  # do not pass messages to ancestor loggers


def get_formatter(aws_request_id: bool, asset_id: bool, app_connection_id: bool):
    return logging.Formatter(
        f'%(asctime)s.%(msecs)03dZ '
        f'{"%(aws_request_id)s " if aws_request_id else ""}'
        f'%(levelname)s '
        f'{"ASSET=%(asset_id)s " if asset_id else ""}'
        f'{"AC=%(app_connection_id)s " if app_connection_id else ""}'
        f'| %(message)s\n',
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
        self.placeholder = placeholder

        self.logging_warning = False
        # one extra message to log the warning
        self.residue_message_count = self.max_message_count + 1

    def emit(self, record) -> None:
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

        extra_chars_count = len(message) - self.max_message_size

        if extra_chars_count <= 0:
            # no need to truncate the message
            return message

        message = (
            message[: len(message) - extra_chars_count - len(self.placeholder)]
            + self.placeholder
        )
        return message

    def log(self, message: str) -> None:
        sys.stdout.write(message)


class LoggingContext(contextlib.ContextDecorator):
    def __init__(
        self,
        aws_request_id: str,
        asset_id: Optional[int],
        app_connection_id: Optional[int],
        handler: logging.Handler,
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

        self.logger = logger

        self.old_handlers = None

    @property
    def asset_id(self) -> Optional[int]:
        return self.filter.asset_id

    @asset_id.setter
    def asset_id(self, value: Optional[int]) -> None:
        self.filter.asset_id = value
        self.set_formatter()

    @property
    def app_connection_id(self) -> Optional[int]:
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
        self.logger.handlers = [self.handler]

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            err_msg = "".join(
                traceback.TracebackException.from_exception(exc_val).format()
            )
            self.logger.error(f'An exception occured: {err_msg}')

        self.logger.handlers = self.old_handlers

        return False  # exception will be propagated

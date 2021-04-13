import contextlib
import logging
import logging.config
import sys
import time
from typing import Optional
from unittest import mock

from corva.configuration import SETTINGS

LOGGER_NAME = 'corva'
CORVA_LOGGER = logging.getLogger(LOGGER_NAME)
CORVA_LOGGER.setLevel(SETTINGS.LOG_LEVEL)
CORVA_LOGGER.propagate = False  # do not pass messages to ancestor loggers
logging.Formatter.converter = time.gmtime  # log time as UTC


@contextlib.contextmanager
def setup_logging(aws_request_id: str, asset_id: int, app_connection_id: Optional[int]):
    CORVA_LOGGER.setLevel(SETTINGS.LOG_LEVEL)

    corva_handler = CorvaLoggerHandler(
        max_message_size=SETTINGS.LOG_THRESHOLD_MESSAGE_SIZE,
        max_message_count=SETTINGS.LOG_THRESHOLD_MESSAGE_COUNT,
        logger=CORVA_LOGGER,
        placeholder=' ...\n',
    )

    corva_handler.setLevel(SETTINGS.LOG_LEVEL)

    # add formatter
    corva_formatter = logging.Formatter(
        f'%(asctime)s.%(msecs)03dZ %(aws_request_id)s %(levelname)s '
        f'ASSET=%(asset_id)s '
        f'{"" if app_connection_id is None else "AC=%(app_connection_id)s "}'
        f'| %(message)s\n',
        '%Y-%m-%dT%H:%M:%S',
    )
    corva_handler.setFormatter(corva_formatter)

    # add filter
    corva_filter = CorvaLoggerFilter(
        aws_request_id=aws_request_id,
        asset_id=asset_id,
        app_connection_id=app_connection_id,
    )
    corva_handler.addFilter(corva_filter)

    with mock.patch.object(CORVA_LOGGER, 'handlers', [corva_handler]):
        yield


class CorvaLoggerFilter(logging.Filter):
    """Injects fields into logging.LogRecord instance for usage in logging.Formatter."""

    def __init__(
        self,
        aws_request_id: str,
        asset_id: int,
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
                f'Disabling the logging as maximum number of logged messages was reached: '
                f'{self.max_message_count}.'
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

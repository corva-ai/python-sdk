import contextlib
import logging
import logging.config
import sys
import time
from typing import Optional
from unittest import mock

from corva.configuration import SETTINGS

LOGGER_NAME = 'corva'
DEFAULT_LOGGER = logging.getLogger(LOGGER_NAME)
DEFAULT_LOGGER.setLevel(SETTINGS.LOG_LEVEL)
DEFAULT_LOGGER.propagate = False  # do not pass messages to ancestor loggers
logging.Formatter.converter = time.gmtime  # log time as UTC


@contextlib.contextmanager
def setup_logging(aws_request_id: str, asset_id: int, app_connection_id: Optional[int]):
    DEFAULT_LOGGER.setLevel(SETTINGS.LOG_LEVEL)

    corva_handler = CorvaLoggerHandler(
        max_chars=SETTINGS.LOG_MAX_CHARS, logger=DEFAULT_LOGGER
    )

    corva_handler.setLevel(SETTINGS.LOG_LEVEL)

    # add formatter
    corva_formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03dZ %(aws_request_id)s %(levelname)s '
        '%(asset_id)s %(app_connection_id)s | %(message)s\n',
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

    with mock.patch.object(DEFAULT_LOGGER, 'handlers', [corva_handler]):
        yield


class CorvaLoggerFilter(logging.Filter):
    def __init__(
        self,
        aws_request_id: str,
        asset_id: int,
        app_connection_id: Optional[int] = None,
    ):
        logging.Filter.__init__(self)

        self.aws_request_id = aws_request_id
        self.asset_id = f'ASSET={asset_id}'
        self.app_connection_id = (
            '' if app_connection_id is None else f'AC={app_connection_id}'
        )

    def filter(self, record):
        record.aws_request_id = self.aws_request_id
        record.asset_id = self.asset_id
        record.app_connection_id = self.app_connection_id

        return True


class CorvaLoggerHandler(logging.Handler):
    def __init__(self, max_chars: int, logger: logging.Logger):
        logging.Handler.__init__(self)

        self.max_chars = max_chars
        self.logger = logger
        self.logged_chars = 0
        self.logging_enabled = True
        self.warning_logged = False

    def emit(self, record):
        if not self.logging_enabled:
            return

        msg = self.format(record)

        self.logged_chars += len(msg)

        if self.logged_chars < self.max_chars:
            sys.stdout.write(msg)
            return

        if self.warning_logged:
            self.logging_enabled = False
            sys.stdout.write(msg)
            return

        msg = f'{msg[: len(msg) - (self.logged_chars - self.max_chars) - 1]}\n'
        sys.stdout.write(msg)

        self.warning_logged = True
        self.logger.warning(
            f'Disabling the logging as maximum number of logged characters was reached: '
            f'{self.max_chars}.'
        )

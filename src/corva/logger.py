from logging import LoggerAdapter, Formatter, getLogger
from logging.config import dictConfig
from time import gmtime

from corva.configuration import SETTINGS


class UtcFormatter(Formatter):
    converter = gmtime


dictConfig(
    {
        'version': 1,
        'formatters': {
            'default': {
                '()': UtcFormatter,
                'format': '%(asctime)s %(name)-5s %(levelname)-5s %(message)s'
            }
        },
        'handlers': {
            'stream': {
                'class': 'logging.StreamHandler',
                'level': SETTINGS.LOG_LEVEL,
                'formatter': 'default',
                'stream': 'ext://sys.stdout'
            }
        },
        'loggers': {
            'main': {
                'level': SETTINGS.LOG_LEVEL,
                'handlers': ['stream'],
                'propagate': False
            }
        }
    }
)


class LogAdapter(LoggerAdapter):
    extra_fields = []

    def process(self, msg, kwargs):
        message_parts = [
            f'[{field}:{self.extra[field]}]'
            for field in self.extra_fields
            if field in self.extra
        ]
        message_parts.append(str(msg))
        message = ' '.join(message_parts)
        return message, kwargs


class AppLogger(LogAdapter):
    extra_fields = ['asset_id']


DEFAULT_LOGGER = getLogger('main')

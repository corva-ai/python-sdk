from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

RETRYABLE_STATUS_CODES = (
    429,  # HTTPStatus.TOO_MANY_REQUESTS
    500,  # HTTPStatus.INTERNAL_SERVER_ERROR
    502,  # HTTPStatus.BAD_GATEWAY
    503,  # HTTPStatus.SERVICE_UNAVAILABLE
    504,  # HTTPStatus.GATEWAY_TIMEOUT
)


def get_retry_strategy(max_retries: int) -> Retry:
    return Retry(
        total=max_retries,
        backoff_factor=0.3,
        status_forcelist=RETRYABLE_STATUS_CODES,
        allowed_methods={'GET', 'POST'},  # Support both
        raise_on_status=False,
    )


def get_requests_session(
        pool_connections_count: int,
        pool_max_size: int,
        pool_block: bool,
        retry_strategy: Optional[Retry] = None,
) -> requests.Session:
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=pool_connections_count,
        pool_maxsize=pool_max_size,
        pool_block=pool_block,
    )

    session = requests.Session()

    session.mount('https://', adapter)
    session.mount('http://', adapter)

    return session

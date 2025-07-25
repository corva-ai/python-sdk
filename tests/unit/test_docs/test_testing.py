import pytest

from docs.modules.ROOT.examples.testing import (
    tutorial001,
    tutorial002,
    tutorial003,
    tutorial004,
    tutorial005,
    tutorial006,
    tutorial007,
)


def test_tutorial001(app_runner):
    tutorial001.test_stream_time_app(app_runner)


def test_tutorial002(app_runner):
    tutorial002.test_stream_depth_app(app_runner)


def test_tutorial003(app_runner):
    tutorial003.test_scheduled_app(app_runner)


def test_tutorial004(app_runner):
    tutorial004.test_task_app(app_runner)


def test_tutorial005(app_runner):
    tutorial005.test_scheduled_app(app_runner)


def test_tutorial006(app_runner):
    tutorial006.test_scheduled_app(app_runner)


def test_tutorial007(app_runner):
    tutorial007.test_task_app(app_runner)


@pytest.mark.skip(
    reason="""
    TODO: review docs/modules/ROOT/examples/testing/tutorial008.py test_reset_cache
     doc example, I believe test behavior doesn't fix real runtime behavior
     After upgrading fakeredis-py lib to 2.26.2 there were some changes at
     2.11.0 that break the test here, see the following links:
     fakeredis changelog:
          https://fakeredis.readthedocs.io/en/latest/about/changelog/#v2110
     corva-sdk docs:
          https://corva-ai.github.io/python-sdk/corva-sdk/1.12.0/index.html#cache
"""
)
def test_tutorial008(app_runner):
    tutorial008.test_reset_cache(app_runner)
    tutorial008.test_reuse_cache(app_runner)

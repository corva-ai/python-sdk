from functools import partial
from unittest.mock import patch, call

import pytest

from corva.app.base import BaseApp
from corva.app.base import ProcessResult
from corva.app.stream import StreamApp
from corva.event.data.stream import StreamEventData, Record
from corva.event.stream import StreamEvent


@pytest.fixture(scope='session')
def record_factory():
    return partial(
        Record,
        timestamp=int(),
        asset_id=int(),
        company_id=int(),
        version=int(),
        data={}
    )


@pytest.fixture(scope='session')
def stream_event_data_factory(record_factory):
    return partial(
        StreamEventData,
        records=[],
        metadata={},
        asset_id=int(),
        app_connection_id=int(),
        app_stream_id=int(),
        is_completed=False
    )


def test__filter_event_data_is_completed(stream_event_data_factory, record_factory):
    event_data = stream_event_data_factory(records=[record_factory()], is_completed=True)
    expected = event_data.copy(update={'records': []}, deep=True)
    assert StreamApp._filter_event_data(data=event_data) == expected

    event_data = stream_event_data_factory(records=[record_factory()], is_completed=False)
    assert StreamApp._filter_event_data(data=event_data) == event_data


def test__filter_event_data_with_last_processed_timestamp(stream_event_data_factory, record_factory):
    last_processed_timestamp = 1
    event_data = stream_event_data_factory(records=[record_factory(timestamp=t) for t in [0, 1, 2]])
    expected = stream_event_data_factory(records=[record_factory(timestamp=2)])
    assert (
         StreamApp._filter_event_data(
             data=event_data, last_processed_timestamp=last_processed_timestamp
         )
         ==
         expected
    )


def test__filter_event_data_with_last_processed_depth(stream_event_data_factory, record_factory):
    last_processed_depth = 1
    event_data = stream_event_data_factory(records=[record_factory(measured_depth=t) for t in [0, 1, 2]])
    expected = stream_event_data_factory(records=[record_factory(measured_depth=2)])
    assert (
         StreamApp._filter_event_data(
             data=event_data, last_processed_depth=last_processed_depth
         )
         ==
         expected
    )


def test_filter_records_with_all_filters(stream_event_data_factory, record_factory):
    last_processed_timestamp = 1
    last_processed_depth = 1
    records = [
        record_factory(timestamp=0, measured_depth=2),
        record_factory(timestamp=1, measured_depth=2),
        record_factory(timestamp=2, measured_depth=2),
        record_factory(timestamp=2, measured_depth=0),
        record_factory(timestamp=2, measured_depth=1),
        record_factory(timestamp=0, measured_depth=2),
    ]
    event_data = stream_event_data_factory(records=records)
    expected = stream_event_data_factory(records=[record_factory(timestamp=2, measured_depth=2)])
    assert (
         StreamApp._filter_event_data(
             data=event_data,
             last_processed_timestamp=last_processed_timestamp,
             last_processed_depth=last_processed_depth
         )
         ==
         expected
    )


def test__filter_event(stream_event_data_factory):
    data = [stream_event_data_factory(asset_id=1), stream_event_data_factory(asset_id=2)]
    event = StreamEvent(data=data)
    with patch.object(
         StreamApp, '_filter_event_data', side_effect=lambda data, **kwargs: data
         ) as _filter_event_data_mock:
        result_event = StreamApp._filter_event(
            event=event,
            last_processed_timestamp=None,
            last_processed_depth=None
        )
    assert _filter_event_data_mock.call_count == 2
    _filter_event_data_mock.assert_has_calls([
        call(data=data[0], last_processed_timestamp=None, last_processed_depth=None),
        call(data=data[1], last_processed_timestamp=None, last_processed_depth=None)
    ])
    assert id(result_event) != id(event)
    assert result_event == event


def test_run(stream_app):
    event = ''
    with patch.object(BaseApp, 'run') as base_run_mock:
        stream_app.run(event=event)
        base_run_mock.assert_called_once_with(
            event=event,
            load_kwargs={'app_key': stream_app.app_key},
            pre_process_kwargs=None,
            process_kwargs=None,
            post_process_kwargs=None,
            on_fail_before_post_process_kwargs=None
        )


def test_run_custom_app_key(stream_app):
    event = ''
    load_kwargs = {'app_key': 'test_run_custom_app_key.app-key'}
    with patch.object(BaseApp, 'run') as base_run_mock:
        stream_app.run(event=event, load_kwargs=load_kwargs)
        base_run_mock.assert_called_once_with(
            event=event,
            load_kwargs=load_kwargs,
            pre_process_kwargs=None,
            process_kwargs=None,
            post_process_kwargs=None,
            on_fail_before_post_process_kwargs=None
        )


def test_pre_process_calls_base(stream_app, stream_event_data_factory, redis):
    event = StreamEvent(data=[stream_event_data_factory()])
    with patch.object(BaseApp, 'pre_process', return_value=ProcessResult(event=event)) as super_pre_process_mock, \
         patch.object(stream_app, '_filter_event'):
        stream_app.pre_process(event=event, state=redis)
    super_pre_process_mock.assert_called_once_with(event=event, state=redis)


def test_pre_process_loads_last_processed_timestamp(stream_app, stream_event_data_factory, redis):
    stream_app.filter_by_timestamp = True

    last_processed_timestamp = 1
    redis.store(key='last_processed_timestamp', value=last_processed_timestamp)

    event = StreamEvent(data=[stream_event_data_factory()])
    with patch.object(BaseApp, 'pre_process', return_value=ProcessResult(event=event)), \
         patch.object(stream_app, '_filter_event') as _filter_event_mock:
        stream_app.pre_process(event=event, state=redis)
    assert _filter_event_mock.call_args[1]['last_processed_timestamp'] == last_processed_timestamp


def test_pre_process_default_last_processed_timestamp(stream_app, stream_event_data_factory, redis):
    stream_app.filter_by_timestamp = False

    event = StreamEvent(data=[stream_event_data_factory()])
    with patch.object(BaseApp, 'pre_process', return_value=ProcessResult(event=event)), \
         patch.object(stream_app, '_filter_event') as _filter_event_mock:
        stream_app.pre_process(event=event, state=redis)
    assert _filter_event_mock.call_args[1]['last_processed_timestamp'] == stream_app.DEFAULT_LAST_PROCESSED_TIMESTAMP


def test_pre_process_loads_last_processed_depth(stream_app, stream_event_data_factory, redis):
    stream_app.filter_by_depth = True

    last_processed_depth = 1
    redis.store(key='last_processed_depth', value=last_processed_depth)

    event = StreamEvent(data=[stream_event_data_factory()])
    with patch.object(BaseApp, 'pre_process', return_value=ProcessResult(event=event)), \
         patch.object(stream_app, '_filter_event') as _filter_event_mock:
        stream_app.pre_process(event=event, state=redis)
    assert _filter_event_mock.call_args[1]['last_processed_depth'] == last_processed_depth


def test_pre_process_default_last_processed_depth(stream_app, stream_event_data_factory, redis):
    stream_app.filter_by_depth = False

    event = StreamEvent(data=[stream_event_data_factory()])
    with patch.object(BaseApp, 'pre_process', return_value=ProcessResult(event=event)), \
         patch.object(stream_app, '_filter_event') as _filter_event_mock:
        stream_app.pre_process(event=event, state=redis)
    assert _filter_event_mock.call_args[1]['last_processed_depth'] == stream_app.DEFAULT_LAST_PROCESSED_DEPTH


def test_pre_process_calls__filter_event(stream_app, stream_event_data_factory, redis):
    event = StreamEvent(data=[stream_event_data_factory()])
    with patch.object(BaseApp, 'pre_process', return_value=ProcessResult(event=event)), \
         patch.object(stream_app, '_filter_event') as _filter_event_mock:
        stream_app.pre_process(event=event, state=redis)
    _filter_event_mock.assert_called_once_with(
        event=event,
        last_processed_timestamp=stream_app.DEFAULT_LAST_PROCESSED_TIMESTAMP,
        last_processed_depth=stream_app.DEFAULT_LAST_PROCESSED_DEPTH
    )


def test_pre_process_return_value(stream_app, stream_event_data_factory, redis):
    event = StreamEvent(data=[stream_event_data_factory()])
    with patch.object(BaseApp, 'pre_process', return_value=ProcessResult(event=event)), \
         patch.object(stream_app, '_filter_event', return_value=event):
        result = stream_app.pre_process(event=event, state=redis)
    assert type(result) == ProcessResult
    assert result == ProcessResult(event=event, next_process_kwargs={})


def test_post_process_calls_base(stream_app, stream_event_data_factory, redis):
    event = StreamEvent(data=[stream_event_data_factory()])
    with patch.object(BaseApp, 'post_process', return_value=ProcessResult(event=event)) as super_post_process_mock:
        stream_app.post_process(event=event, state=redis)
    super_post_process_mock.assert_called_once_with(event=event, state=redis)


def test_post_process_correct_last_processed_timestamp(
     stream_app, stream_event_data_factory, redis, record_factory
):
    records1 = [record_factory(timestamp=1)]
    records2 = [record_factory(timestamp=2)]
    data1 = stream_event_data_factory(records=records1)
    data2 = stream_event_data_factory(records=records2)
    event = StreamEvent(data=[data1, data2])
    with patch.object(BaseApp, 'post_process', return_value=ProcessResult(event=event)), \
         patch.object(redis, 'store') as store_mock:
        stream_app.post_process(event=event, state=redis)
    assert store_mock.call_args[1]['mapping']['last_processed_timestamp'] == 2


def test_post_process_correct_last_processed_timestamp_none_in_records(
     stream_app, stream_event_data_factory, redis, record_factory
):
    records = [record_factory(timestamp=None)]
    data = stream_event_data_factory(records=records)
    event = StreamEvent(data=[data])
    with patch.object(BaseApp, 'post_process', return_value=ProcessResult(event=event)), \
         patch.object(redis, 'store') as store_mock:
        stream_app.post_process(event=event, state=redis)
    assert (
         store_mock.call_args[1]['mapping']['last_processed_timestamp']
         ==
         stream_app.DEFAULT_LAST_PROCESSED_TIMESTAMP
    )


def test_post_process_correct_last_processed_timestamp_empty_records(
     stream_app, stream_event_data_factory, redis, record_factory
):
    data1 = stream_event_data_factory(records=[])
    data2 = stream_event_data_factory(records=[])
    event = StreamEvent(data=[data1, data2])
    with patch.object(BaseApp, 'post_process', return_value=ProcessResult(event=event)), \
         patch.object(redis, 'store') as store_mock:
        stream_app.post_process(event=event, state=redis)
    assert (
         store_mock.call_args[1]['mapping']['last_processed_timestamp']
         ==
         stream_app.DEFAULT_LAST_PROCESSED_TIMESTAMP
    )


def test_post_process_correct_last_processed_depth(
     stream_app, stream_event_data_factory, redis, record_factory
):
    records1 = [record_factory(measured_depth=1)]
    records2 = [record_factory(measured_depth=2)]
    data1 = stream_event_data_factory(records=records1)
    data2 = stream_event_data_factory(records=records2)
    event = StreamEvent(data=[data1, data2])
    with patch.object(BaseApp, 'post_process', return_value=ProcessResult(event=event)), \
         patch.object(redis, 'store') as store_mock:
        stream_app.post_process(event=event, state=redis)
    assert store_mock.call_args[1]['mapping']['last_processed_depth'] == 2


def test_post_process_correct_last_processed_depth_none_in_records(
     stream_app, stream_event_data_factory, redis, record_factory
):
    records = [record_factory(measured_depth=None)]
    data = stream_event_data_factory(records=records)
    event = StreamEvent(data=[data])
    with patch.object(BaseApp, 'post_process', return_value=ProcessResult(event=event)), \
         patch.object(redis, 'store') as store_mock:
        stream_app.post_process(event=event, state=redis)
    assert store_mock.call_args[1]['mapping']['last_processed_depth'] == stream_app.DEFAULT_LAST_PROCESSED_DEPTH


def test_post_process_correct_last_processed_depth_empty_records(
     stream_app, stream_event_data_factory, redis, record_factory
):
    records1 = []
    records2 = []
    data1 = stream_event_data_factory(records=records1)
    data2 = stream_event_data_factory(records=records2)
    event = StreamEvent(data=[data1, data2])
    with patch.object(BaseApp, 'post_process', return_value=ProcessResult(event=event)), \
         patch.object(redis, 'store') as store_mock:
        stream_app.post_process(event=event, state=redis)
    assert store_mock.call_args[1]['mapping']['last_processed_depth'] == stream_app.DEFAULT_LAST_PROCESSED_DEPTH


def test_post_process_store_call(
     stream_app, stream_event_data_factory, redis, record_factory
):
    event = StreamEvent(data=[stream_event_data_factory()])
    with patch.object(BaseApp, 'post_process', return_value=ProcessResult(event=event)), \
         patch.object(redis, 'store') as store_mock:
        stream_app.post_process(event=event, state=redis)
    store_mock.assert_called_once_with(mapping={'last_processed_timestamp': -1, 'last_processed_depth': -1})


def test_post_process_return_value(
     stream_app, stream_event_data_factory, redis, record_factory
):
    event = StreamEvent(data=[stream_event_data_factory()])
    with patch.object(BaseApp, 'post_process', return_value=ProcessResult(event=event)):
        result = stream_app.post_process(event=event, state=redis)
    assert result == ProcessResult(event=event, next_process_kwargs={})

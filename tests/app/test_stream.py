import pytest
from pytest_mock import MockerFixture
from redis import Redis

from corva.models.stream import StreamContext, Record, StreamEventData
from corva.app.stream import StreamApp
from corva.event import Event
from tests.conftest import APP_KEY, CACHE_URL


@pytest.fixture(scope='function')
def stream_app(api):
    return StreamApp(api=api, app_key=APP_KEY, cache_url=CACHE_URL)


@pytest.fixture(scope='module')
def stream_event_data_factory(record_factory):
    def _stream_event_data_factory(**kwargs):
        default_params = {
            'records': [],
            'metadata': {},
            'asset_id': int(),
            'app_connection_id': int(),
            'app_stream_id': int(),
            'is_completed': False
        }
        default_params.update(kwargs)

        return StreamEventData(**default_params)

    return _stream_event_data_factory


@pytest.fixture(scope='module')
def record_factory():
    def _record_factory(**kwargs):
        default_params = {
            'timestamp': int(),
            'asset_id': int(),
            'company_id': int(),
            'version': int(),
            'data': {},
            'collection': str()
        }
        default_params.update(kwargs)

        return Record(**default_params)

    return _record_factory


@pytest.fixture(scope='function')
def stream_context_factory(stream_event_data_factory, redis):
    def _stream_context_factory(**kwargs):
        default_params = {
            'event': Event([stream_event_data_factory()]),
            'state': redis
        }
        default_params.update(kwargs)

        return StreamContext(**default_params)

    return _stream_context_factory


@pytest.mark.parametrize(
    'attr_name,expected', (('DEFAULT_LAST_PROCESSED_VALUE', -1), ('group_by_field', 'app_connection_id'))
)
def test_default_values(attr_name, expected):
    assert getattr(StreamApp, attr_name) == expected


def test__filter_event_data_is_completed(stream_event_data_factory, record_factory):
    # is_completed True
    event_data = stream_event_data_factory(records=[record_factory()], is_completed=True)
    expected = event_data.copy(update={'records': []}, deep=True)
    assert StreamApp._filter_event_data(data=event_data) == expected

    # is_completed False
    event_data = stream_event_data_factory(records=[record_factory()], is_completed=False)
    assert StreamApp._filter_event_data(data=event_data) == event_data


def test__filter_event_data_with_last_processed_timestamp(stream_event_data_factory, record_factory):
    last_processed_timestamp = 1
    event_data = stream_event_data_factory(records=[record_factory(timestamp=t) for t in [0, 1, 2]])
    expected = event_data.copy(update={'records': [event_data.records[2]]}, deep=True)

    assert (
         StreamApp._filter_event_data(
             data=event_data, last_processed_timestamp=last_processed_timestamp
         )
         ==
         expected
    )


def test__filter_event_data_with_last_processed_depth(stream_event_data_factory, record_factory):
    last_processed_depth = 1
    event_data = stream_event_data_factory(records=[record_factory(measured_depth=d) for d in [0, 1, 2]])
    expected = event_data.copy(update={'records': [event_data.records[2]]}, deep=True)

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
    expected = event_data.copy(update={'records': [event_data.records[2]]}, deep=True)

    assert (
         StreamApp._filter_event_data(
             data=event_data,
             last_processed_timestamp=last_processed_timestamp,
             last_processed_depth=last_processed_depth
         )
         ==
         expected
    )


def test__filter_event(mocker: MockerFixture, stream_event_data_factory):
    data = [stream_event_data_factory(asset_id=1), stream_event_data_factory(asset_id=2)]
    event = Event(data)

    _filter_event_data_mock = mocker.patch.object(
        StreamApp, '_filter_event_data', side_effect=lambda data, **kwargs: data
    )

    result_event = StreamApp._filter_event(
        event=event,
        last_processed_timestamp=None,
        last_processed_depth=None
    )

    assert _filter_event_data_mock.call_count == 2
    _filter_event_data_mock.assert_has_calls([
        mocker.call(data=data[0], last_processed_timestamp=None, last_processed_depth=None),
        mocker.call(data=data[1], last_processed_timestamp=None, last_processed_depth=None)
    ])
    assert id(result_event) != id(event)
    assert result_event == event


def test_pre_process_loads_last_processed_timestamp(mocker: MockerFixture, stream_app, stream_context_factory):
    stream_app.filter_by_timestamp = True
    last_processed_timestamp = 1
    context = stream_context_factory()

    context.state.store(key='last_processed_timestamp', value=last_processed_timestamp)

    _filter_event_spy = mocker.spy(stream_app, '_filter_event')

    stream_app.pre_process(context=context)

    assert _filter_event_spy.call_args[1]['last_processed_timestamp'] == last_processed_timestamp


def test_pre_process_default_last_processed_timestamp(mocker: MockerFixture, stream_app, stream_context_factory):
    stream_app.filter_by_timestamp = False
    context = stream_context_factory()

    _filter_event_spy = mocker.spy(stream_app, '_filter_event')

    stream_app.pre_process(context=context)

    assert _filter_event_spy.call_args[1]['last_processed_timestamp'] == stream_app.DEFAULT_LAST_PROCESSED_VALUE


def test_pre_process_last_processed_timestamp_none(mocker: MockerFixture, stream_app, stream_context_factory):
    stream_app.filter_by_timestamp = True
    context = stream_context_factory()

    _filter_event_spy = mocker.spy(stream_app, '_filter_event')

    stream_app.pre_process(context=context)

    assert _filter_event_spy.call_args[1]['last_processed_timestamp'] == stream_app.DEFAULT_LAST_PROCESSED_VALUE


def test_pre_process_loads_last_processed_depth(mocker: MockerFixture, stream_app, stream_context_factory):
    stream_app.filter_by_depth = True
    context = stream_context_factory()
    last_processed_depth = 1

    context.state.store(key='last_processed_depth', value=last_processed_depth)

    _filter_event_spy = mocker.spy(stream_app, '_filter_event')

    stream_app.pre_process(context=context)

    assert _filter_event_spy.call_args[1]['last_processed_depth'] == last_processed_depth


def test_pre_process_default_last_processed_depth(mocker: MockerFixture, stream_app, stream_context_factory):
    stream_app.filter_by_depth = False
    context = stream_context_factory()

    _filter_event_spy = mocker.spy(stream_app, '_filter_event')
    stream_app.pre_process(context=context)

    assert _filter_event_spy.call_args[1]['last_processed_depth'] == stream_app.DEFAULT_LAST_PROCESSED_VALUE


def test_pre_process_last_processed_depth_none(mocker: MockerFixture, stream_app, stream_context_factory):
    stream_app.filter_by_depth = True
    context = stream_context_factory()

    _filter_event_spy = mocker.spy(stream_app, '_filter_event')

    stream_app.pre_process(context=context)

    assert _filter_event_spy.call_args[1]['last_processed_depth'] == stream_app.DEFAULT_LAST_PROCESSED_VALUE


def test_pre_process_calls__filter_event(mocker: MockerFixture, stream_app, stream_context_factory):
    context = stream_context_factory()

    _filter_event_spy = mocker.spy(stream_app, '_filter_event')

    stream_app.pre_process(context=context)

    _filter_event_spy.assert_called_once_with(
        event=context.event,
        last_processed_timestamp=stream_app.DEFAULT_LAST_PROCESSED_VALUE,
        last_processed_depth=stream_app.DEFAULT_LAST_PROCESSED_VALUE
    )


def test_post_process_correct_last_processed_timestamp(
     mocker: MockerFixture, stream_app, stream_event_data_factory, record_factory, stream_context_factory
):
    records1 = [record_factory(timestamp=1)]
    records2 = [record_factory(timestamp=2)]
    data1 = stream_event_data_factory(records=records1)
    data2 = stream_event_data_factory(records=records2)
    event = Event([data1, data2])
    context = stream_context_factory(event=event)

    store_spy = mocker.spy(context.state, 'store')

    stream_app.post_process(context=context)

    assert store_spy.call_args[1]['mapping']['last_processed_timestamp'] == 2


def test_post_process_correct_last_processed_timestamp_none_or_empty_records(
     mocker: MockerFixture, stream_app, stream_event_data_factory, record_factory, stream_context_factory
):
    data1 = stream_event_data_factory(records=[record_factory(timestamp=None)])
    data2 = stream_event_data_factory(records=[])
    event = Event([data1, data2])
    context = stream_context_factory(event=event)

    mock = mocker.patch.object(Redis, 'hset')
    store_spy = mocker.spy(context.state, 'store')

    stream_app.post_process(context=context)

    assert 'last_processed_timestamp' not in store_spy.call_args[1]['mapping']
    mock.assert_called_once()


def test_post_process_correct_last_processed_depth(
     mocker: MockerFixture, stream_app, stream_event_data_factory, record_factory, stream_context_factory
):
    records1 = [record_factory(measured_depth=1)]
    records2 = [record_factory(measured_depth=2)]
    data1 = stream_event_data_factory(records=records1)
    data2 = stream_event_data_factory(records=records2)
    event = Event([data1, data2])
    context = stream_context_factory(event=event)

    store_spy = mocker.spy(context.state, 'store')

    stream_app.post_process(context=context)

    assert store_spy.call_args[1]['mapping']['last_processed_depth'] == 2


def test_post_process_correct_last_processed_depth_none_or_empty_records(
     mocker: MockerFixture, stream_app, stream_event_data_factory, record_factory, stream_context_factory
):
    data1 = stream_event_data_factory(records=[record_factory(measured_depth=None)])
    data2 = stream_event_data_factory(records=[])
    event = Event([data1, data2])
    context = stream_context_factory(event=event)

    mock = mocker.patch.object(Redis, 'hset')
    store_spy = mocker.spy(context.state, 'store')

    stream_app.post_process(context=context)

    assert 'last_processed_depth' not in store_spy.call_args[1]['mapping']
    mock.assert_called_once()


def test_post_process_store_call(
     mocker: MockerFixture, stream_app, stream_event_data_factory, record_factory, stream_context_factory
):
    event = Event([stream_event_data_factory()])
    context = stream_context_factory(event=event)

    mock = mocker.patch.object(Redis, 'hset')
    store_spy = mocker.spy(context.state, 'store')

    stream_app.post_process(context=context)

    store_spy.assert_called_once_with(mapping={})
    mock.assert_called_once()

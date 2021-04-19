Corva python-sdk is a framework for building
[Corva DevCenter][dev-center-docs] apps.

## Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [App types](#app-types)
- [Event](#event)
- [Api](#api)
- [Cache](#cache)
- [Logging](#logging)  
- [Testing](#testing)  
- [Contributing](#contributing)

## Requirements

Python 3.8+

## Installation

```console
$ pip install corva-sdk
```

## App types

There are three app types that you can build:

1. `stream` - works with real-time data
2. `scheduled` - works with data at defined schedules/intervals
   (e.g. once an hour)
3. `task` - works with data on-demand

**Note**: Use type hints like those in the examples below for better support from editors and tools.

#### Stream

Stream apps can be of two types - time or depth based.

###### Stream time app

```python
from corva import Api, Cache, StreamTimeEvent, stream


@stream
def stream_time_app(event: StreamTimeEvent, api: Api, cache: Cache):
    # get some data from the api
    drillstrings = api.get_dataset(
        provider='corva',
        dataset='data.drillstring',
        query={
            'asset_id': event.asset_id,
        },
        sort={'timestamp': 1},
        limit=1,
    )

    # do some calculations/modifications to the data
    for drillstring in drillstrings:
        drillstring['id'] = drillstring.pop('_id')

    # save the data to private collection
    api.post(path='api/v1/data/my_provider/my_collection/', data=drillstrings)
```

###### Stream depth app

```python
from corva import Api, Cache, StreamDepthEvent, stream


@stream
def stream_depth_app(event: StreamDepthEvent, api: Api, cache: Cache):
    # get some data from the api
    drillstrings = api.get_dataset(
        provider='corva',
        dataset='data.drillstring',
        query={
            'asset_id': event.asset_id,
        },
        sort={'timestamp': 1},
        limit=1,
    )

    # do some calculations/modifications to the data
    for drillstring in drillstrings:
        drillstring['id'] = drillstring.pop('_id')

    # save the data to private collection
    api.post(path='api/v1/data/my_provider/my_collection/', data=drillstrings)
```

#### Scheduled

```python
from corva import Api, Cache, ScheduledEvent, scheduled


@scheduled
def scheduled_app(event: ScheduledEvent, api: Api, cache: Cache):
    print('Hello, World!')
```

#### Task

```python
from corva import Api, TaskEvent, task


@task
def task_app(event: TaskEvent, api: Api):
    print('Hello, World!')
```

## Event

An event is an object that contains data for an app function to process.
`event` instance is inserted automatically as a first parameter to each app type.
There are different event types for every app type: 
`StreamTimeEvent`, `StreamDepthEvent`, `ScheduledEvent` and `TaskEvent`.

## Api

Apps might need to communicate with the
[Corva Platform API][corva-api] and [Corva Data API][corva-data-api]. 
This SDK provides an `Api` class, which wraps the Python `requests`
library and adds automatic authorization, convenient URL usage and 
reasonable timeouts to API requests.
`Api` instance is inserted automatically as a second parameter to each app type.
`Api` supports following HTTP methods: `GET`, `POST`, `PATCH`, `PUT`
and `DELETE`.

#### Examples:

```python
from corva import Api, scheduled


@scheduled
def my_app(event, api: Api, cache):
    # Corva API calls
    api.get('/v2/pads', params={'param': 'val'})
    api.post('/v2/pads', data={'key': 'val'})
    api.patch('/v2/pads/123', data={'key': 'val'})
    api.delete('/v2/pads/123')

    # Corva Data API calls
    api.get('/api/v1/data/provider/dataset/', params={'param': 'val'})
    api.post('/api/v1/data/provider/dataset/', data={'key': 'val'})
    api.put('/api/v1/data/provider/dataset/', data={'key': 'val'})
    api.delete('/api/v1/data/provider/dataset/')
 ```

## Cache

Apps might need to share some data between runs. The sdk provides a `Cache` class, that allows you to store, load and do
other operations with data.
`Cache` instance is inserted automatically as a third parameter to `stream` and `scheduled` apps. <br>
**Note**: `task` apps don't get a `Cache` parameter as they aren't meant to store data between invokes.<br>

`Cache` uses a dict-like database, so the data is stored as `key:value` pairs.
`key` should be of `str` type, and `value` can have any of the following types: `str`, `int`, `float` and `bytes`.

#### Examples:

1. Store and load:
   ```python
   from corva import Cache, scheduled
   
   
   @scheduled
   def my_app(event, api, cache: Cache):
       cache.store(key='key', value='val')
       # cache: {'key': 'val'}
   
       cache.load(key='key')  # returns 'val'
   ```

2. Store and load multiple:
   ```python
   from corva import Cache, scheduled
   
   
   @scheduled
   def my_app(event, api, cache: Cache):
       cache.store(mapping={'key1': 'val1', 'key2': 'val2'})
       # cache: {'key1': 'val1', 'key2': 'val2'}
   
       cache.load_all()  # returns {'key1': 'val1', 'key2': 'val2'}
   ```

3. Delete and delete all:
   ```python
   from corva import Cache, scheduled
   
   
   @scheduled
   def my_app(event, api, cache: Cache):
       cache.store(mapping={'key1': 'val1', 'key2': 'val2', 'key3': 'val3'})
       # cache: {'key1': 'val1', 'key2': 'val2', 'key3': 'val3'}
   
       cache.delete(keys=['key1'])
       # cache: {'key2': 'val2', 'key3': 'val3'}
   
       cache.delete_all()
       # cache: {}
   ```
   
4. Expiry, ttl and exists. **Note**: by default `Cache` sets an expiry to 60 days.
   ```python
   import time

   from corva import Cache, scheduled
   
   
   @scheduled
   def my_app(event, api, cache: Cache):
       cache.store(key='key', value='val', expiry=60)  # 60 seconds
       # cache: {'key': 'val'}
   
       cache.ttl()  # 60 seconds
       cache.pttl()  # 60000 milliseconds
       cache.exists()  # True
   
       time.sleep(60)  # wait 60 seconds
       # cache: {}
   
       cache.exists()  # False
       cache.ttl()  # -2: doesn't exist
       cache.pttl()  # -2: doesn't exist
   ```

## Logging
As apps are executed very frequently
(once a second or so),
unlimited logging can lead to huge amounts of data.

The SDK provides a `Logger` object,
which is a safe way for logging in apps.

The `Logger` is a `logging.Logger` instance
and should be used like every other Python logger.

The `Logger` has following features:
1. Log messages are injected with contextual information,
   which makes it easy to filter through logs
   while debugging issues.
1. Log message length is limited. 
   Too long messages are truncated to not exceed the limit. 
   Set by `LOG_THRESHOLD_MESSAGE_SIZE` env variable. 
   Default value is `1000` symbols or bytes.
2. Number of log messages is limited.
   After reaching the limit logging gets disabled.
   Set by `LOG_THRESHOLD_MESSAGE_COUNT` env variable.
   Default value is `15`.
3. Logging level can be set using `LOG_LEVEL` env variable.
   Default value is `INFO`.

#### Logger usage example

```python3
from corva import Logger, stream


@stream
def stream_app(event, api, cache):
    Logger.debug('Debug message!')
    Logger.info('Info message!')
    Logger.warning('Warning message!')
    Logger.error('Error message!')
    try:
        0 / 0
    except ZeroDivisionError:
        Logger.exception('Exception message!')
```


## Testing

Testing Corva applications is easy and enjoyable.

The SDK provides convenient tools for testing through 
`pytest` [plugin][pytest-plugin].
Write your tests using `pytest` to get the access to the plugin.
To install the library run `pip install pytest`. 

#### Stream time app example test

```python3
from corva import StreamTimeEvent, StreamTimeRecord, stream


@stream
def stream_app(event, api, cache):
    return 'Hello, World!'


def test_stream_time_app(app_runner):
    event = StreamTimeEvent(
        asset_id=0, company_id=0, records=[StreamTimeRecord(timestamp=0)]
    )

    result = app_runner(stream_app, event=event)

    assert result == 'Hello, World!'
```

#### Stream depth app example test

```python
from corva import StreamDepthEvent, StreamDepthRecord, stream


@stream
def stream_app(event, api, cache):
    return 'Hello, World!'


def test_stream_depth_app(app_runner):
    event = StreamDepthEvent(
        asset_id=0, company_id=0, records=[StreamDepthRecord(measured_depth=0)]
    )

    result = app_runner(stream_app, event=event)

    assert result == 'Hello, World!'
```

#### Scheduled app example test

```python3
from corva import ScheduledEvent, scheduled


@scheduled
def scheduled_app(event, api, cache):
    return 'Hello, World!'


def test_scheduled_app(app_runner):
    event = ScheduledEvent(asset_id=0, start_time=0, end_time=0)

    result = app_runner(scheduled_app, event=event)

    assert result == 'Hello, World!'
```

#### Task app example test

```python
from corva import TaskEvent, task


@task
def task_app(event, api):
    return 'Hello, World!'


def test_task_app(app_runner):
    event = TaskEvent(asset_id=0, company_id=0)

    result = app_runner(task_app, event=event)

    assert result == 'Hello, World!'
```

## Contributing

#### Set up the project

```console
$ cd ~/YOUR_PATH/python-sdk
$ python -m venv venv
$ source venv/bin/activate
(venv) $ pip install -e .[dev]
```

#### Run tests

```console
(venv) $ pytest
```

#### Run code linter

```console
(venv) $ flake8
```

[dev-center-docs]: https://app.corva.ai/dev-center/docs

[corva-api]: https://api.corva.ai/documentation/index.html

[corva-data-api]: https://data.corva.ai/docs#/

[pytest-plugin]: https://docs.pytest.org/en/stable/writing_plugins.html

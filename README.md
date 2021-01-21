Corva python-sdk is a framework for building stream, scheduled and task apps.

## Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [App types](#app-types)
- [Event](#event)
- [Api](#api)
- [Cache](#cache)
- [Contributing](#contributing)

## Requirements

Python 3.8+

## Installation

```console
$ pip install corva-sdk
```

## App types

There are three app types, that you can build: `stream`, `scheduled` and `task`.<br>
**Note**: it is recommended to use type hints like in examples below,
so that editors and tools can give you better support.

#### Stream

```python
from corva import Api, Cache, Corva, StreamEvent


# 1 define a function with required parameters, that will be provided by sdk
def stream_app(event: StreamEvent, api: Api, cache: Cache):
    """Main logic function"""
    pass


# 2 define a function that will be run by AWS lambda
def lambda_handler(event, context):
    """AWS lambda handler"""
    corva = Corva()  # 3 initialize Corva
    corva.stream(stream_app, event)  # 4 run stream_app by passing it and event to Corva.stream
```

`Corva.stream` provides two additional parameters:

- `filter_by_timestamp` - if set to `True` will take the latest processed
  `timestamp` from cache and filter out records from event with 
  either smaller or same `timestamp`;
- `filter_by_depth` - if set to `True` will take the latest processed
  `measured_depth` from cache and filter out records from event with
  either smaller or same `measured_depth`.

#### Scheduled

```python
from corva import Api, Cache, Corva, ScheduledEvent


def scheduled_app(event: ScheduledEvent, api: Api, cache: Cache):
    pass


def lambda_handler(event, context):
    corva = Corva()
    corva.scheduled(scheduled_app, event)
```

#### Task (will be added soon)

```python
from corva import Api, Corva, TaskEvent


# note, that task app doesn't receive cache parameter
def task_app(event: TaskEvent, api: Api):
    pass


def lambda_handler(event, context):
    corva = Corva()
    corva.task(task_app, event)
```

## Event

Event is what triggers app execution.
It contains necessary data for app to run e.g. `asset_id`, that triggered the event.
Every app type receives `Event` instance as a first parameter.
Each app type has its own event type: `StreamEvent`, `ScheduledEvent` and `TaskEvent`. <br>
**Note**: It is recommended to use event classes as type hints,
so that editors and tools can give you better support.

#### Example:

```python
from corva import Api, Cache, Corva, StreamEvent


def stream_app(event: StreamEvent, api: Api, cache: Cache):
    event.records[0].measured_depth  # get measured depth of first event record


def lambda_handler(event, context):
    corva = Corva()
    corva.stream(stream_app, event)
```

## Api

The apps might need to communicate with Corva API and Corva Data API.
The sdk provides an `Api` class - a thin wrapper around `requests`
library that handles authorization, adds timeouts and retries to request.
`Api` instance is inserted automatically as a second parameter to each app type.
`Api` supports following HTTP methods: `GET`, `POST`, `PATCH`, `PUT` and `DELETE`.<br>
**Note**: It is recommended to use `Api` class as type hint, 
so that editors and tools can give you better support.

#### Examples:

```python
from corva import Api, Corva


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


def lambda_handler(event, context):
    corva = Corva()
    corva.scheduled(my_app, event)
 ```

## Cache

Sometimes apps need to share some data between runs.
The sdk provides a `Cache` class that allows you to store, load and
do other operations with data.
`Cache` instance is inserted automatically as a last parameter
to `stream` and `scheduled` apps. <br>
**Note**: `task` apps don't get `Cache` parameter as they aren't meant to store
the data between invokes.<br>

`Cache` uses a dict-like database, so the data is stored as `key:value` pairs.
`key` should be of `str` type, and `value` can be any of
the following types: `str`, `int`, `float`, `bytes`.<br>
**Note**: It is recommended to use `Cache` class as type hint,
so that editors and tools can give you better support.

#### Examples:

1. Store and load:
   ```python
   from corva import Cache, Corva
   
   
   def my_app(event, api, cache: Cache):
      cache.store(key='key', value='val')
      # cache: {'key': 'val'}
      
      cache.load(key='key')  # returns 'val'
   
   
   def lambda_handler(event, context):
      corva = Corva()
      corva.scheduled(my_app, event)
   ```

2. Store and load multiple:
   ```python
   from corva import Cache, Corva
   
   
   def my_app(event, api, cache: Cache):
      cache.store(mapping={'key1': 'val1', 'key2': 'val2'})
      # cache: {'key1': 'val1', 'key2': 'val2'}
      
      cache.load_all()  # returns {'key1': 'val1', 'key2': 'val2'}
   
   
   def lambda_handler(event, context):
      corva = Corva()
      corva.scheduled(my_app, event)
   ```

3. Delete and delete all:
   ```python
   from corva import Cache, Corva

   
   def my_app(event, api, cache: Cache):
      cache.store(mapping={'key1': 'val1', 'key2': 'val2', 'key3': 'val3'})
      # cache: {'key1': 'val1', 'key2': 'val2', 'key3': 'val3'}
      
      cache.delete(keys=['key1'])
      # cache: {'key2': 'val2', 'key3': 'val3'}
      
      cache.delete_all()
      # cache: {}
   
   
   def lambda_handler(event, context):
      corva = Corva()
      corva.scheduled(my_app, event)
   ```
4. Expiry, ttl and exists. **Note**: by default `Cache` sets an expiry to 60 days.
   ```python
   import time
   from corva import Cache, Corva
   
   
   def my_app(event, api, cache: Cache):
      cache.store(key='key', value='val', expiry=60)  # 60 seconds
      # cache: {'key': 'val'}
      
      cache.ttl()  # 60 seconds
      cache.pttl() # 60000 milliseconds
      cache.exists()  # True
      
      time.sleep(60)  # wait 60 seconds
      # cache: {}
      
      cache.exists()  # False
      cache.ttl()  # -2: doesn't exist
      cache.pttl() # -2: doesn't exist
   
   
   def lambda_handler(event, context):
      corva = Corva()
      corva.scheduled(my_app, event)
   ```

## Contributing

#### Set up the project

```console
$ cd ~/YOUR_PATH/python-sdk
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

#### Run tests

```console
$ venv/bin/python3 -m pytest tests
```

#### Run code linter

```console
$ venv/bin/python3 -m flake8
```

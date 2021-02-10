Corva python-sdk is a framework for building
[Corva DevCenter][dev-center-docs] apps.

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

There are three app types that you can build:

1. `stream` - works with real-time data
2. `scheduled` - works with data at defined schedules/intervals
   (e.g. once an hour)
3. `task` - works with data on-demand

**Note**: Use type hints like those in the examples below for better support from editors and tools.

#### Stream

```python
import json

from corva import Api, Cache, Corva, StreamEvent


def stream_app(event: StreamEvent, api: Api, cache: Cache):
    # get some data from api
    drillstrings = api.get(
        'api/v1/data/corva/data.drillstring/',
        params={
            'query': json.dumps({'asset_id': event.asset_id, }),
            'sort': json.dumps({'timestamp': 1}),
            'limit': 1}
    ).json()  # List[dict]

    # do some calculations/modifications to the data
    for drillstring in drillstrings:
        drillstring['id'] = drillstring.pop('_id')

    # save the data to private collection
    api.post(
        f'api/v1/data/my_provider/my_collection/',
        data=drillstrings
    )


def lambda_handler(event, context):
    corva = Corva(context)
    corva.stream(stream_app, event)
```

`Corva.stream` provides an optional parameter:
- `filter_mode` - set to `timestamp` or `depth` to clear [event](#event) 
  data with previously processed `timestamp` or `measured_depth`.

#### Scheduled

```python
from corva import Api, Cache, Corva, ScheduledEvent


def scheduled_app(event: ScheduledEvent, api: Api, cache: Cache):
    pass


def lambda_handler(event, context):
    corva = Corva(context)
    corva.scheduled(scheduled_app, event)
```

## Event

An event is an object that contains data for an app function to process.
`event` instance is inserted automatically as a first parameter to each app type. There are different event types for
every app type: `StreamEvent`, `ScheduledEvent` and `TaskEvent`.

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
    corva = Corva(context)
    corva.scheduled(my_app, event)
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
   from corva import Cache, Corva
   
   
   def my_app(event, api, cache: Cache):
      cache.store(key='key', value='val')
      # cache: {'key': 'val'}
      
      cache.load(key='key')  # returns 'val'
   
   
   def lambda_handler(event, context):
      corva = Corva(context)
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
      corva = Corva(context)
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
      corva = Corva(context)
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
      corva = Corva(context)
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

[dev-center-docs]: https://app.corva.ai/dev-center/docs

[corva-api]: https://api.corva.ai/documentation/index.html

[corva-data-api]: https://data.corva.ai/docs#/

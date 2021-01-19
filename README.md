Corva python-sdk is a framework for building stream, scheduled and task apps.

## Requirements

Python 3.8+

* [Pydantic](https://github.com/samuelcolvin/pydantic)
* [Redis](https://pypi.org/project/redis/)
* [Requests](https://pypi.org/project/requests/)
* [urllib3](https://pypi.org/project/urllib3/)

## Installation

```console
$ pip install corva-sdk
```

## Apps examples
There are three app types, that you can develop: `stream`, `scheduled`, `task`.

#### Examples

1. **Stream**
   
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

2. **Scheduled**

   ```python
   from corva import Api, Cache, Corva, ScheduledEvent
   
   
   def scheduled_app(event: ScheduledEvent, api: Api, cache: Cache):
       pass
   
   
   def lambda_handler(event, context):
       corva = Corva()
       corva.scheduled(scheduled_app, event)
   ```

3. **Task** (will be added soon)

   ```python
   from corva import Api, Cache, Corva, TaskEvent
   
   
   # note, that task app doesn't receive cache parameter
   def task_app(event: TaskEvent, api: Api):
       pass
   
   
   def lambda_handler(event, context):
       corva = Corva()
       corva.task(task_app, event)
   ```

## Event

Event is what triggers app execution. It contains necessary data for app to run e.g. `asset_id`, that triggered the
event. Every app type receives `Event` instance as a first parameter. There are three event types matched to app
types: `StreamEvent`, `ScheduledEvent`, `TaskEvent`. It is recommened to use these classes as type hints, like in app
examples above, so that it is easier to discover, what fields each event type has.

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

The apps might need to communicate with Corva API and Corva Data API. The sdk provides an `Api` class - a thin wrapper
around `requests`
library that handles authorization, adds timeouts and retries to request.
`Api` instance is inserted automatically as a second parameter to each app type.
`Api` supports following HTTP methods: `GET`, `POST`, `PATCH`, `PUT`, `DELETE`.

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

Sometimes apps need to share some data between runs. The sdk provides a `Cache` class that allows you to store, load and
do other operations with data.
`Cache` instance is inserted automatically as a last parameter to `stream` and `scheduled` apps. <br>
**Note**: `task` apps don't get `Cache` parameter as they aren't meant to store the data between invokes.

`Cache` uses a dict-like database, so the data is stored as `key:value` pairs.
`key` should be of `str` type, and `value` can be any of the following types: `str`, `int`, `float`, `bytes`.

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

## Development - contributing

### Set up the project

```console
$ cd ~/YOUR_PATH/python-sdk
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

### Run tests

```console
$ venv/bin/python3 -m pytest tests
```

### Run code linter

```console
$ venv/bin/python3 -m flake8
```

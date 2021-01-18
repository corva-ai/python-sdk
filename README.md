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

## Examples

### Stream app

```python
from corva import Api, Cache, Corva, StreamEvent


# 1 define a function with required parameters, that will be provided by sdk
def stream_app(event: StreamEvent, api: Api, cache: Cache):
    """Main logic function"""
    pass


    # 2 define a function that will be run by AWS lambda
def lambda_handler(event, context):
    """AWS lambda handler"""
    app = Corva()  # 3 initialize Corva
    app.stream(stream_app, event)  # 4 run stream_app by passing it to Corva.stream alongside with event
```

### Scheduled app

```python
from corva import Api, Cache, Corva, ScheduledEvent


def scheduled_app(event: ScheduledEvent, api: Api, cache: Cache):
    pass


def lambda_handler(event, context):
    app = Corva()
    app.scheduled(scheduled_app, event)
```

### Task app (will be added soon)

```python
from corva import Api, Cache, Corva, TaskEvent


def task_app(event: TaskEvent, api: Api):
    pass


def lambda_handler(event, context):
    app = Corva()
    app.task(task_app, event)
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

# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.14.1] - 2025-07-25
### Added
- Session mechanism for significantly decrease number of an http load on data-api for apps with intensive calling 
- Added possibility to adjust some params related to connection pool
  - `POOL_CONNECTIONS_COUNT`: Total pools count
  - `POOL_MAX_SIZE`: Max connections count per pool/host 
  - `POOL_BLOCK`: Wait until connection released or not (instantly raise an exception)
  - `MAX_RETRY_COUNT`: If 0 then retires will be disabled, otherwise retrying logic will be used
- Move retrying logic from `tenacity` to internal `urllib3.util.Retry(...)`
- Removed redundant dependency `tenacity` from `python-sdk`
- Bump version for `py3.13` to `py3.13.3` at CI version matrix in order to fix broken tests for logging
- Bump version for `fakeredis` to fix some tests


## [1.14.0] - 2025-04-17
### Fixed
- merge_events parameter for scheduled data time apps should result in correct start/end times in a final app event.

## [1.13.1] - 2025-02-17
### Fixed
- Documentation 404 at GitHub pages


## [1.13.0] - 2025-02-11
### Added
- Upgrade a bunch of outdated core dependencies + test & lint dependencies


## [1.12.1] - 2025-01-27
### Fixed
- Add app logic for meaningful error when mismatched app type is used either declared at `manifest.json` or according 
  to used event payload passed to app


## [1.12.0] - 2024-10-06
### Fixed
- Unset the `CORVA_LOGGER.propagate = False`, so the OTel handler will be able to collect and send those logs as well
  refs https://github.com/corva-ai/otel/pull/37


## [1.11.4] - 2024-02-08
### Fixed
- Now "version" field represents not schema version but rather the version of rerun and can be any integer.


## [1.11.3] - 2024-02-07
### Fixed
- Filter out records with `None` data from stream time records.


## [1.11.2] - 2024-01-05

### Fixed
- Most of the fields in partial merge events should be optional.


## [1.11.1] - 2023-12-18

### Added
- Fields `partition` and `rerun_partition` are not expected in partial merge event payload.
- Data structure under `data` key  of partial merge event payload can accept additional fields.


## [1.11.0] - 2023-12-07

### Added
- Optional merge_events parameter to @stream and @scheduled decorators. Default is False, if True - merge all incoming events into one.
- Added functionality to make retries for the failed HTTP requests.


# [1.10.0] - 2023-11-08

### Added
-  New handler to process partial rerun merge events


## [1.9.2] - 2023-10-25

### Fixed
-  issue with `insert_data` method not working when trying to insert data into any dataset.

### Changed
- Limit number of available Cache connections to 1


## [1.9.1] - 2023-05-09

### Fixed
- `urllib3` v2 not supported by AWS Lambda.


## [1.9.0] - 2023-03-06

### Added
- `log_identifier` field to stream depth events.

### Changed
- Reraise exceptions in `task` apps
instead of suppressing them.


## [1.8.1] - 2023-01-24

### Added
- `id` field to rerun data.


## [1.8.0] - 2022-11-28

### Added
- `insert_data` method to `Api`.
See `Insert data` section in docs.
- Example usage for followable apps. 
See `Followable apps` section in docs.


## [1.7.0] - 2022-11-08

### Added
- `produce_messages` method to `Api`.
See `Produce messages` section in docs.
- Documentation on followable apps. 
See `Followable apps` section in docs.


## [1.6.0] - 2022-10-19

### Added
- `cache` parameter to `app_runner`
to reuse the cache object.


## [1.5.3] - 2022-09-05

### Changed
- Set status as completed
for failed scheduled natural time apps. 


## [1.5.2] - 2022-07-11

### Fixed
- GitHub Pages


## [1.5.1] - 2022-07-11

### Fixed
- GitHub Pages


## [1.5.0] - 2022-07-11

### Added
- Rerun support.
`StreamTimeEvent`, `StreamDepthEvent`,
`ScheduledDataTimeEvent`, `ScheduledDepthEvent`
and `ScheduledNaturalTimeEvent` got new
`rerun` field which stores rerun metadata.


## [1.4.0] - 2022-04-25

### Added
- `Cache` methods:
  * `set_many`
  * `get_many`
  * `get_all`
  * `delete_many`
  * `delete_all`

### Fixed
- Wrong log formatting in Dev Center:
multiline Python logs (like stack traces)
being split into multiple CloudWatch log entries.

### Changed
- Log internal SDK errors using
`warning` level instead of `exception`.

## [1.4.0-rc.1] - 2022-02-15

### Added
- `get`, `set` and new signature for `delete` methods to `Cache`.
See docs for more info.

### Deprecated
- `Cache` methods:
  * old `delete` signature
  * `store`
  * `load`
  * `load_all`
  * `delete_all`
  * `ttl`
  * `pttl`
  * `exists`

### Fixed
- `Cache` unable to set unique expiry for each key.


## [1.3.1] - 2022-04-14

### Fixed
- Wrong log formatting in Dev Center:
multiline Python logs (like stack traces)
being split into multiple CloudWatch log entries.


## [1.3.0] - 2022-02-15

### Added
- Secrets support.
See corresponding section in docs.


## [1.2.2] - 2021-11-01

### Fixed
- Successful task app failing to update task status.


## [1.2.1] - 2021-10-29

### Deprecated 
- Returning dict result from task app
to get it stored in task payload.
Send the request to update the payload explicitly in your app.


## [1.2.0] - 2021-08-09

### Added
- Subtypes of scheduled event:
`ScheduledDataTimeEvent`, `ScheduledDepthEvent` and `ScheduledNaturalTimeEvent`  

### Deprecated
- `ScheduledEvent` usage


## [1.1.0] - 2021-07-19

### Added
- Ability to pass custom logging handlers
to app decorators using `handler` keyword argument.
- Natural time event (single dict) parsing in `scheduled` apps.

### Fixed
- Multiple logging of the same exception in stream and scheduled apps.
- Log message truncation for low `LOG_THRESHOLD_MESSAGE_SIZE` values.


## [1.0.3] - 2021-06-27


## [1.0.2] - 2021-06-27

### Added
- Logging of internal errors.


## [1.0.1] - 2021-04-30

### Fixed
- AWS Lambda context parsing.


## [1.0.0] - 2021-04-29

### Added
- `corva.stream`, `corva.scheduled` and `corva.task` app decorators. 
  See readme for usage examples.
- `ScheduledEvent.company_id` field.  

### Removed
- `corva.Corva` class.


## [0.0.18] - 2021-04-16

### Changed
- Events are allowed to have extra fields.


## [0.0.17] - 2021-04-15

### Added
- `corva.Logger` object, that should be used for app logging.
- `LOG_THRESHOLD_MESSAGE_SIZE` and `LOG_THRESHOLD_MESSAGE_COUNT`
env variables, that should be used to configure logging.


## [0.0.16] - 2021-04-02

### Added
- `app_runner` fixture for testing apps.

### Changed
- `StreamEvent` was split into `StreamTimeEvent` and `StreamDepthEvent`,
  which have corresponding `StreamTimeRecord` and `StreamDepthRecord` records.
- Deleted all unsued fields from `ScheduledEvent`, `TaskEvent`,
  `StreamTimeEvent` and `StreamDepthEvent`.

### Removed
- `filter_mode` parameter from `Corva.stream`.
  Filtering is now automatic.


## [0.0.15] - 2021-03-23

### Added
- `Corva.task` decorator for task apps.


## [0.0.14] - 2021-03-12

### Added
- `Testing` section to `README.md`.
- `Api.get_dataset` method.

### Changed
- `ScheduledEvent.schedule_end` field is now optional.
- `ScheduledEvent.schedule_end` and `ScheduledEvent.schedule_start` field types
  from `datetime` to `int`.


## [0.0.13] - 2021-03-04

### Added
- Tools for testing apps.


## [0.0.12] - 2021-02-11

### Fixed
- `TaskEvent` queue event parsing.

###Changed
- `StreamEvent` must have at least one record.
- `StreamEvent` and `ScheduledEvent`:
  - Added descriptions to fields.
  - Simplified event structures.


## [0.0.11] - 2021-02-10

### Fixed
- `ScheduledEvent` queue event parsing.


## [0.0.10] - 2021-02-05

### Changed
- `Api` class:
  - Deleted retries.
  - Responses do not use `raise_for_status` anymore.
  - Lowered `default_timeout` to 30 seconds.
  - Fixed url build exceptions on Windows.


## [0.0.9] - 2021-02-05

### Removed
- Obsolete `StreamEvent` fields: `app_version`.
- Obsolete `ScheduledEvent` fields: `app_version`.


## [0.0.8] - 2021-02-05

### Fixed
- `api_key` extraction from `context`.


## [0.0.7] - 2021-02-04 

### Fixed
- `StreamEvent` queue event parsing.


## [0.0.5] - 2021-02-04 

### Added
- Required `context` parameter to `Corva`.
- Documentation in `README.md`.


## [0.0.4] - 2021-01-20

### Added
- `Corva` class, which contains `stream` and `scheduled` decorators
  for stream and scheduled apps.

### Removed
- `StreamApp` and `ScheduledApp` classes.


## [0.0.3] - 2020-12-15

### Fixed
- Deployment to PyPI.


## [0.0.2] - 2020-12-15

###Added
- `StreamApp` to build stream apps.
- `ScheduledApp` to build scheduled apps.
- `TaskApp` to build task apps.
- `Api` class to access Platform and Data Corva APIs.
- `Cache` class to share data between app invokes.
- Event classes: `StreamEvent`, `ScheduledEvent` and `TaskEvent`.


[Unreleased] https://github.com/corva-ai/python-sdk/compare/v1.14.0...master
[1.14.0] https://github.com/corva-ai/python-sdk/compare/v1.13.1...v1.14.0
[1.13.1] https://github.com/corva-ai/python-sdk/compare/v1.13.0...v1.13.1
[1.13.0] https://github.com/corva-ai/python-sdk/compare/v1.12.1...v1.13.0
[1.12.1] https://github.com/corva-ai/python-sdk/compare/v1.12.0...v1.12.1
[1.12.0] https://github.com/corva-ai/python-sdk/compare/v1.11.4...v1.12.0
[1.11.4] https://github.com/corva-ai/python-sdk/compare/v1.11.3...v1.11.2
[1.11.3] https://github.com/corva-ai/python-sdk/compare/v1.11.2...v1.11.3
[1.11.2] https://github.com/corva-ai/python-sdk/compare/v1.11.1...v1.11.2
[1.11.1] https://github.com/corva-ai/python-sdk/compare/v1.11.0...v1.11.1
[1.11.0] https://github.com/corva-ai/python-sdk/compare/v1.10.0...v1.11.0
[1.10.0] https://github.com/corva-ai/python-sdk/compare/v1.9.2...v1.10.0
[1.9.2] https://github.com/corva-ai/python-sdk/compare/v1.9.1...v1.9.2
[1.9.1] https://github.com/corva-ai/python-sdk/compare/v1.9.0...v1.9.1
[1.9.0] https://github.com/corva-ai/python-sdk/compare/v1.8.1...v1.9.0
[1.8.1] https://github.com/corva-ai/python-sdk/compare/v1.8.0...v1.8.1
[1.8.0] https://github.com/corva-ai/python-sdk/compare/v1.7.0...v1.8.0
[1.7.0] https://github.com/corva-ai/python-sdk/compare/v1.6.0...v1.7.0
[1.6.0] https://github.com/corva-ai/python-sdk/compare/v1.5.3...v1.6.0
[1.5.3] https://github.com/corva-ai/python-sdk/compare/v1.5.2...v1.5.3
[1.5.2] https://github.com/corva-ai/python-sdk/compare/v1.5.1...v1.5.2
[1.5.1] https://github.com/corva-ai/python-sdk/compare/v1.5.0...v1.5.1
[1.5.0] https://github.com/corva-ai/python-sdk/compare/v1.4.0...v1.5.0
[1.4.0] https://github.com/corva-ai/python-sdk/compare/v1.4.0-rc.1...v1.4.0
[1.4.0-rc.1] https://github.com/corva-ai/python-sdk/compare/v1.3.1...v1.4.0-rc.1
[1.3.1] https://github.com/corva-ai/python-sdk/compare/v1.3.0...v1.3.1
[1.3.0] https://github.com/corva-ai/python-sdk/compare/v1.2.2...v1.3.0
[1.2.2] https://github.com/corva-ai/python-sdk/compare/v1.2.1...v1.2.2
[1.2.1] https://github.com/corva-ai/python-sdk/compare/v1.2.0...v1.2.1
[1.2.0] https://github.com/corva-ai/python-sdk/compare/v1.1.0...v1.2.0
[1.1.0] https://github.com/corva-ai/python-sdk/compare/v1.0.3...v1.1.0
[1.0.3] https://github.com/corva-ai/python-sdk/compare/v1.0.2...v1.0.3
[1.0.2] https://github.com/corva-ai/python-sdk/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/corva-ai/python-sdk/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/corva-ai/python-sdk/compare/v0.0.18...v1.0.0
[0.0.18]: https://github.com/corva-ai/python-sdk/compare/v0.0.17...v0.0.18
[0.0.17]: https://github.com/corva-ai/python-sdk/compare/v0.0.16...v0.0.17
[0.0.16]: https://github.com/corva-ai/python-sdk/compare/v0.0.15...v0.0.16
[0.0.15]: https://github.com/corva-ai/python-sdk/compare/v0.0.14...v0.0.15
[0.0.14]: https://github.com/corva-ai/python-sdk/compare/v0.0.13...v0.0.14
[0.0.13]: https://github.com/corva-ai/python-sdk/compare/v0.0.12...v0.0.13
[0.0.12]: https://github.com/corva-ai/python-sdk/compare/v0.0.11...v0.0.12
[0.0.11]: https://github.com/corva-ai/python-sdk/compare/v0.0.10...v0.0.11
[0.0.10]: https://github.com/corva-ai/python-sdk/compare/v0.0.9...v0.0.10
[0.0.9]: https://github.com/corva-ai/python-sdk/compare/v0.0.8...v0.0.9
[0.0.8]: https://github.com/corva-ai/python-sdk/compare/v0.0.7...v0.0.8
[0.0.7]: https://github.com/corva-ai/python-sdk/compare/v0.0.5...v0.0.7
[0.0.5]: https://github.com/corva-ai/python-sdk/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/corva-ai/python-sdk/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/corva-ai/python-sdk/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/corva-ai/python-sdk/releases/tag/v0.0.2

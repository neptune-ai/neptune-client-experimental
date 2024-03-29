## [UNRELEASED] neptune-experimental 0.3.5

### Changes
- Do not include trashed runs at default ([#61](https://github.com/neptune-ai/neptune-client-experimental/pull/61))


## neptune-experimental 0.3.4

### Changes
- Support for `neptune` 1.9.1 ([#56](https://github.com/neptune-ai/neptune-client-experimental/pull/56))


## neptune-experimental 0.3.3

### Changes
- Introduce `ProccessorStopEventListener` to group log messages from `PartitionedOperationProcessor` ([#44](https://github.com/neptune-ai/neptune-client-experimental/pull/44))
- Adjustment after matching UI capabilities ([#53](https://github.com/neptune-ai/neptune-client-experimental/pull/53))
- Adjustments after resource management ([#49](https://github.com/neptune-ai/neptune-client-experimental/pull/49))

### Fixes
- Fix operation counting and `atexit` handling in `ProccessorStopEventListener` ([#50](https://github.com/neptune-ai/neptune-client-experimental/pull/50))


## neptune-experimental 0.3.2

### Changes
- Allow up to 128 characters for custom run id ([#46](https://github.com/neptune-ai/neptune-client-experimental/pull/46))


## neptune-experimental 0.3.1

### Changes
- Exposed `ReadOnlyRun` object and added a fallback for `project` param in `ReadOnlyProject` ([#32](https://github.com/neptune-ai/neptune-client-experimental/pull/32))
- Adjustments after no bravado deserialization ([#33](https://github.com/neptune-ai/neptune-client-experimental/pull/33))
- Adjustments after adding support for more than 10k entries for `fetch_*_table()` ([#35](https://github.com/neptune-ai/neptune-client-experimental/pull/35))


## neptune-experimental 0.3.0

### Features
- Added default run name handling ([#18](https://github.com/neptune-ai/neptune-client-experimental/pull/18))

### Changes
- Add custom units and descriptions to tqdm progress bars ([#29](https://github.com/neptune-ai/neptune-client-experimental/pull/29))
- Increased default step size when fetching runs ([#30](https://github.com/neptune-ai/neptune-client-experimental/pull/30))


## neptune-experimental 0.2.1

### Changes
- Added support for download progress update handling in fetching API ([#25](https://github.com/neptune-ai/neptune-client-experimental/pull/25))
- Adjustments after no synchronization callbacks fix ([#20](https://github.com/neptune-ai/neptune-client-experimental/pull/20))
- Renamed methods in `ProgressUpdateHandler` ([#26](https://github.com/neptune-ai/neptune-client-experimental/pull/26))


## neptune-experimental 0.2.0

### Features
- Added fetching API ([#19](https://github.com/neptune-ai/neptune-client-experimental/pull/19))


## neptune-experimental 0.1.0

### Features
- Added support for sending operations in parallel ([#15](https://github.com/neptune-ai/neptune-client-experimental/pull/15))
- Safety (errors suppressing) execution mode ([#8](https://github.com/neptune-ai/neptune-client-experimental/pull/8))

### Fixes
- Fixed patching with `OperationErrorProcessor` ([#12](https://github.com/neptune-ai/neptune-client-experimental/pull/12))


## neptune-experimental 0.0.2

### Changes
- Split overriding into multiple methods ([#10](https://github.com/neptune-ai/neptune-client-experimental/pull/10))


## neptune-experimental 0.0.1

### Changes
- Allow to disable handling of remote signals ([#2](https://github.com/neptune-ai/neptune-client-experimental/pull/2))
- Added support for multiple features ([#3](https://github.com/neptune-ai/neptune-client-experimental/pull/3))
- Sample logging for series errors ([#4](https://github.com/neptune-ai/neptune-client-experimental/pull/4))

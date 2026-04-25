# `tap-bigquery`

BigQuery tap class.

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| project_id          | True     | None    | GCP Project |
| credentials_path    | False    | None    | The path to the service account credentials file. |
| filter_schemas      | False    | None    | If an array of schema names is provided, the tap will only process the specified BigQuery schemas and ignore others. If left blank, the tap automatically determines ALL available BigQuery schemas. |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |
| batch_config        | False    | None    |             |

A full list of supported settings and capabilities is available by running: `tap-bigquery --about`


$ poetry run tap-bigquery --about --format=markdown
# `tap-bigquery`

Google BigQuery tap.

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| project_id          | True     | None    | GCP Project |
| credentials_path    | False    | None    | The path to the service account credentials file. |
| filter_schemas      | False    | None    | If an array of schema names is provided, the tap will only process the specified BigQuery schemas and ignore others. If left blank, the tap automatically determines ALL available BigQuery schemas. |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |
| batch_config        | False    | None    |             |

A full list of supported settings and capabilities is available by running: `tap-bigquery --about`

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

Credential resolution order (Application Default Credentials–style):

1. **Config** `google_application_credentials` – JSON string, JSON object, or path to a service account key file.
2. **Env** `GOOGLE_APPLICATION_CREDENTIALS_STRING` – JSON string of the service account key (e.g. secret injected by Airflow).
3. **Env** `GOOGLE_APPLICATION_CREDENTIALS` – Path to a service account key file.
4. **Application Default Credentials (ADC)** – No config or env set: workload identity (GKE/Cloud Run/Airflow pod), `gcloud auth application-default login`, or metadata server.

For Airflow/GKE with an attached service account, omit `google_application_credentials` and do not set the env vars; the tap uses ADC.

## Usage

You can easily run `tap-bigquery` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-bigquery --version
tap-bigquery --help
tap-bigquery --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-bigquery` CLI interface directly using `poetry run`:

```bash
poetry run tap-bigquery --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-bigquery
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-bigquery --version
# OR run a test `elt` pipeline:
meltano elt tap-bigquery target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.

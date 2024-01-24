import pytest
import json
import tap_bigquery.client

@pytest.mark.parametrize("input, output", [
    ()
])
def test_transform_record(input: dict, output: dict) -> None:
    assert json.dumps(tap_bigquery.client.transform_record(input)) == json.dumps(output)
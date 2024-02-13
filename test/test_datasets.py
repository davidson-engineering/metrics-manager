import pytest
from datetime import datetime


def test_random_dataset_1(random_dataset_1):
    assert len(random_dataset_1) == 100
    assert random_dataset_1[0]["name"] == "random_dataset"
    assert random_dataset_1[0]["value"] < 1
    assert random_dataset_1[0]["value"] > 0


def test_random_dataset_1_chunked(random_dataset_1_chunked):
    assert len(random_dataset_1_chunked) == 10
    assert len(random_dataset_1_chunked[0]) == 10
    assert random_dataset_1_chunked[0][0]["name"] == "random_dataset"
    assert random_dataset_1_chunked[0][0]["value"] < 1
    assert random_dataset_1_chunked[0][0]["value"] > 0


def test_random_dataset(random_dataset_1_timed):
    assert len(random_dataset_1_timed) == 1000
    assert random_dataset_1_timed[0].name == "random_dataset"
    assert random_dataset_1_timed[0].value < 1
    assert random_dataset_1_timed[0].value > 0
    assert random_dataset_1_timed[0].time == datetime(2021, 1, 1, 0, 0, 0)


def test_random_dataset_2_chunked(random_dataset_2_chunked):
    assert len(random_dataset_2_chunked) == 10
    assert len(random_dataset_2_chunked[0]) == 100
    assert random_dataset_2_chunked[0][0]["name"] == "random_dataset"
    assert random_dataset_2_chunked[0][0]["value"] < 1
    assert random_dataset_2_chunked[0][0]["value"] > 0


def test_random_dataset_3_chunked(random_dataset_3_chunked):
    assert len(random_dataset_3_chunked) == 10
    assert len(random_dataset_3_chunked[0]) == 1000
    assert random_dataset_3_chunked[0][0]["name"] == "random_dataset"
    assert random_dataset_3_chunked[0][0]["value"] < 1
    assert random_dataset_3_chunked[0][0]["value"] > 0

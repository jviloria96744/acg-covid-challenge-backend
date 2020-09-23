import datetime
import json
import pandas as pd
import pytest
from python_etl import transform_data


@pytest.fixture()
def base_jh_data():
    base_jh_data_dict = {
        "Date": ["2020-01-22", "2020-01-22", "2020-01-23", "2020-01-24"],
        "Country/Region": ["UK", "US", "US", "US"],
        "Province/State": ["", "", "", ""],
        "Lat": [0, 0, 0, 0],
        "Long": [0, 0, 0, 0],
        "Confirmed": [1, 2, 3, 4],
        "Recovered": [1, 1, 1, 1],
        "Deaths": [0, 1, 0, 1]
    }

    return pd.DataFrame(data=base_jh_data_dict)


def test_filter_jh_data_column_names(base_jh_data):
    filtered_data = transform_data.filter_jh_data(base_jh_data)
    assert list(filtered_data.columns) == ["Date", "Recovered"]


def test_filter_jh_data_row_count(base_jh_data):
    filtered_data = transform_data.filter_jh_data(base_jh_data)
    assert len(filtered_data) == 3


@pytest.fixture()
def neg_jh_data():
    negative_jh_data_dict_1 = {
        "Date": ["2020-01-22"],
        "Country/Region": ["UK"],
        "Province/State": [""],
        "Lat": [0],
        "Long": [0],
        "Confirmed": [1],
        "Recovered": [1],
        "Deaths": [0]
    }

    negative_jh_data_dict_2 = {
        "Date": ["2020-01-22"],
        "Country/Region": ["US"],
        "Province/State": [""],
        "Lat": [0],
        "Long": [0],
        "Confirmed": [1],
        "Recovereds": [1],
        "Deaths": [0]
    }

    return {
        "negative_jh_data_1": pd.DataFrame(
            data=negative_jh_data_dict_1),
        "negative_jh_data_2": pd.DataFrame(
            data=negative_jh_data_dict_2),
    }


test_cases = [
    ('negative_jh_data_1'),
    ('negative_jh_data_2')
]


@pytest.mark.parametrize("body", test_cases)
def test_filter_jh_data_negative(body, neg_jh_data):
    with pytest.raises(Exception):
        filtered_data = transform_data.filter_jh_data(neg_jh_data[body])


@pytest.fixture()
def base_nyt_data():
    base_nyt_data_dict = {
        "date": ["2020-01-23", "2020-01-24"],
        "cases": [0, 1],
        "deaths": [0, 0]
    }

    return pd.DataFrame(data=base_nyt_data_dict)


@pytest.fixture()
def filtered_jh_data():
    filtered_base_jh_data = {
        "Date": ["2020-01-22", "2020-01-23", "2020-01-24"],
        "Recovered": [1, 1, 1]
    }

    return pd.DataFrame(data=filtered_base_jh_data)


def test_merge_data_column_names(base_nyt_data, filtered_jh_data):
    merged_data = transform_data.merge_data(base_nyt_data, filtered_jh_data)

    assert list(merged_data.columns) == [
        "date", "cases", "deaths", "recoveries"]


def test_merge_data_row_count(base_nyt_data, filtered_jh_data):
    merged_data = transform_data.merge_data(base_nyt_data, filtered_jh_data)

    assert len(merged_data) == 2


@pytest.fixture()
def neg_filtered_jh_data():
    negative_filtered_base_jh_data = {
        "Date": ["2020-01-22"],
        "Recovered": [1]
    }

    return pd.DataFrame(data=negative_filtered_base_jh_data)


def test_merge_data_negative(base_nyt_data, neg_filtered_jh_data):
    with pytest.raises(Exception):
        merged_data = transform_data.merge_data(
            base_nyt_data, neg_filtered_jh_data)


@pytest.fixture()
def base_merged_data():
    base_merged_data_dict = {
        "date": ["2020-01-23", "2020-01-24"],
        "cases": [0, 1],
        "deaths": [0, 0],
        "recoveries": [0, 1.0]
    }

    return pd.DataFrame(data=base_merged_data_dict)


def test_data_validity(base_merged_data):
    processed_data = transform_data.check_count_validity(base_merged_data)

    assert processed_data.loc[1, "recoveries"] == 1


@pytest.fixture()
def neg_merged_data():

    negative_merged_data_1 = {
        "date": ["2020-01-23", "2020-01-24"],
        "cases": ['a', 1],
        "deaths": [0, 0],
        "recoveries": [0, 1.0]
    }

    negative_merged_data_2 = {
        "date": ["2020-01-23", "2020-01-24"],
        "cases": [0, 1],
        "deaths": [0, -1],
        "recoveries": [0, 1.0]
    }

    return {
        "negative_merged_data_1": pd.DataFrame(data=negative_merged_data_1),
        "negative_merged_data_2": pd.DataFrame(data=negative_merged_data_2)
    }


test_cases = [
    ('negative_merged_data_1'),
    ('negative_merged_data_2')
]


@pytest.mark.parametrize("body", test_cases)
def test_data_validity_negative(body, neg_merged_data):
    with pytest.raises(Exception):
        processed_data = transform_data.check_count_validity(
            neg_merged_data[body])


def test_transform_data(base_nyt_data, base_jh_data):
    covid_data, new_records, updated_records = transform_data.transform_data(
        base_nyt_data, base_jh_data, None)

    assert covid_data.loc[1, "cases-diff"] == 1


def test_transform_data_day_of_week(base_nyt_data, base_jh_data):
    covid_data, new_records, updated_records = transform_data.transform_data(
        base_nyt_data, base_jh_data, None)

    assert covid_data.loc[0, "day_of_week"] == "Thursday"


def test_transform_data_date_type(base_nyt_data, base_jh_data):
    covid_data, new_records, updated_records = transform_data.transform_data(
        base_nyt_data, base_jh_data, None)

    assert isinstance(covid_data.loc[0, "date"], datetime.date)


def test_transform_data_column_names(base_nyt_data, base_jh_data):
    covid_data, new_records, updated_records = transform_data.transform_data(
        base_nyt_data, base_jh_data, None)

    assert list(covid_data.columns) == ['date', 'cases', 'deaths', 'recoveries',
                                        'date-diff', 'day_of_week', 'cases-diff', 'deaths-diff', 'recoveries-diff']


def test_transform_data_row_count(base_nyt_data, base_jh_data):
    covid_data, new_records, updated_records = transform_data.transform_data(
        base_nyt_data, base_jh_data, None)

    assert len(covid_data) == 2


def test_transform_data_new_record_count(base_nyt_data, base_jh_data):
    covid_data, new_records, updated_records = transform_data.transform_data(
        base_nyt_data, base_jh_data, None)

    assert len(new_records) == 2


def test_transform_data_updated_record_count(base_nyt_data, base_jh_data):
    covid_data, new_records, updated_records = transform_data.transform_data(
        base_nyt_data, base_jh_data, None)

    assert len(updated_records) == 0


@pytest.fixture()
def prev_data():
    prev_data_dict = {
        "date": ["2020-01-23"],
        "cases": [0],
        "deaths": [10],
        "recoveries": [0],
        "date-diff": [0],
        "day_of_week": ["Wednesday"],
        "cases-diff": [0],
        "deaths-diff": [0],
        "recoveries-diff": [0]
    }

    return pd.DataFrame(data=prev_data_dict)


def test_transform_data_new_record_count_prev_data(base_nyt_data, base_jh_data, prev_data):
    covid_data, new_records, updated_records = transform_data.transform_data(
        base_nyt_data, base_jh_data, prev_data)

    assert len(new_records) == 1


def test_transform_data_updated_record_count_prev_data(base_nyt_data, base_jh_data, prev_data):
    covid_data, new_records, updated_records = transform_data.transform_data(
        base_nyt_data, base_jh_data, prev_data)

    assert len(updated_records) == 1

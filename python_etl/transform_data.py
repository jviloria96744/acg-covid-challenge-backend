from datetime import date
import logging
import pandas as pd
from json_logger import setup_logging

setup_logging(logging.INFO)
logger = logging.getLogger()


def transform_data(ny_times_data, jh_data, prev_data):
    jh_data = filter_jh_data(jh_data)

    covid_data = merge_data(ny_times_data, jh_data)
    covid_data = check_count_validity(covid_data)

    if prev_data is None:
        new_records = sorted(list(covid_data["date"].unique()))
        updated_records = []
    else:
        try:
            new_records, updated_records = get_changed_records(
                covid_data, prev_data)
        except:
            logger.error(
                "Error while getting updated/changed records, previous data and current data are not comparable")
            raise

    try:
        covid_data["date"] = covid_data["date"].apply(date.fromisoformat)
    except ValueError:
        logger.error(
            "Invalid date field format, field must be in YYYY-MM-DD format")
        raise

    try:
        covid_data = add_new_fields(covid_data)
    except:
        logger.error("Error adding new fields")
        raise

    logger.info("Data Transformations Complete")
    return covid_data, new_records, updated_records


def filter_jh_data(jh_data):
    try:
        jh_data = jh_data[jh_data["Country/Region"] == "US"]
        jh_data = jh_data[["Date", "Recovered"]]
    except:
        extra_data = {
            "Column Names": jh_data.columns
        }
        logger.error("Error filtering Johns Hopkins Data",
                     extra=dict(data=extra_data))
        raise

    if len(jh_data) == 0:
        logger.error("Johns Hopkins Data has no US data")
        raise Exception

    return jh_data


def merge_data(nyt_data, jh_data):
    try:
        covid_data = pd.merge(
            nyt_data, jh_data, left_on="date", right_on="Date")
        covid_data.drop(columns=["Date"], inplace=True)
        covid_data.columns = ["date", "cases", "deaths", "recoveries"]
    except:
        extra_data = {
            "NYT_Columns": nyt_data.columns,
            "JH_Columns": jh_data.columns,
        }
        logger.error("Error merging data", extra=dict(data=extra_data))
        raise

    if len(covid_data) == 0:
        logger.error("Merged data contains no records")
        raise Exception

    return covid_data


def check_count_validity(covid_data):
    for col in ["cases", "deaths", "recoveries"]:
        try:
            covid_data[col] = covid_data[col].apply(int)
        except:
            logger.error(f"{col} contains non-numeric data")
            raise

        min_value = min(covid_data[col])
        if min_value < 0:
            logger.error(f"{col} contains data with negative values")
            raise

    return covid_data


def get_changed_records(covid_data, prev_data):
    for col in ["cases", "deaths", "recoveries"]:
        prev_data[col] = prev_data[col].apply(int)

    comparison_data = covid_data.merge(prev_data, on=["date"], how="left")

    new_records = comparison_data[comparison_data["cases_y"].isnull()]
    new_records = list(new_records["date"].unique())

    comparison_data = comparison_data[comparison_data["cases_y"].notnull()]

    updated_records = []
    for item in ["cases", "deaths", "recoveries"]:
        comparison_data[f"{item}_y"] = comparison_data[f"{item}_y"].apply(int)
        comparison_data[f"{item}_diff"] = (
            comparison_data[f"{item}_x"] - comparison_data[f"{item}_y"]
        )
        diff = comparison_data[comparison_data[f"{item}_diff"] != 0]
        updated_records = updated_records + list(diff["date"].unique())

    updated_records = list(set(updated_records))

    return new_records, updated_records


def add_new_fields(covid_data):
    day_of_week_dict = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }

    covid_data["date-diff"] = covid_data["date"].diff()
    covid_data.loc[0, "date-diff"] = pd.Timedelta(value=0, unit="days")
    covid_data["date-diff"] = covid_data["date-diff"].apply(lambda x: x.days)

    covid_data["day_of_week"] = covid_data["date"].apply(
        lambda x: day_of_week_dict[x.weekday()])

    for col in ["cases", "deaths", "recoveries"]:
        covid_data[f"{col}-diff"] = covid_data[col].diff()
        covid_data.loc[0, f"{col}-diff"] = 0
        covid_data[f"{col}-diff"] = covid_data[f"{col}-diff"].apply(int)

    return covid_data

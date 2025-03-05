import sys
import os
import errno
import datetime
import requests
import time
import json
import pandas as pd

from common.config import Config as config
from common import log
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


def calculate_number_of_intervals_per_day(interval):
    number_per_unit = {"m": 24 * 60, "h": 24, "d": 1}
    return int(number_per_unit[interval[-1]] / int(interval[:-1]))


def calculate_number_of_intervals_per_year(interval):
    return calculate_number_of_intervals_per_day(interval) * 365


def interval_in_seconds(interval):
    number_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 60 * 60 * 24,
    }
    return int(number_per_unit[interval[-1]] * int(interval[:-1]))


def create_directory(dir) -> str:
    """
    디렉토리를 생성한다. 이미 존재하는 디렉토리라면 무시.\n
    경로는 프로젝트의 시작 / 경로를 포함하는 절대 경로를 사용. ex) /backtest_result\n
    return: 생성한(이미 생성된) 디렉토리의 경로
    """
    create_directory = os.path.dirname(os.path.realpath(__file__)) + "/.." + dir
    try:
        os.makedirs(create_directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e

    return create_directory


def datetime_to_timestamp(dt):
    dt = dt.replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp()) * 1000


def convert_to_original(str):
    if str.isdigit():
        # 만약 음수일 경우 isdigit이 False이므로 float으로 변환된다.
        return int(str)
    elif isinstance(json.loads(str), list) or isinstance(json.loads(str), dict):
        return json.loads(str)
    else:
        try:
            return float(str)
        except ValueError:
            return str


def now_ts() -> int:
    return datetime.datetime.now().timestamp() * 1000


def variable_to_string(variable):
    return ", ".join([":".join(map(str, t)) for t in variable])


def variable_to_filename(variable):
    if not variable:
        return "", False

    filename = "_".join(["-".join(map(str, t)) for t in variable])
    filename += "_"

    if len(filename) > 50:
        return f"{len(variable)}_variables_", True

    return filename, False


enable_slack = False

def send_slack(message: str, mention: str = None):
    if not enable_slack:
        return

    cfg = config.slack()

    headers = {"content-type": "application/json"}

    params = {}
    params["channel"] = cfg["Channel"]
    params["text"] = message
    if mention:
        params["mention"] = "author_" + mention

    r = requests.post(cfg["Url"], headers=headers, data=json.dumps(params))
    if r.status_code != 200:
        log.makeLogger("slack_noti").error(f"Failed to send slack. status={r.status_code} message={message}")


def save_multi_summary(strategy_name, results):

    error_results = pd.DataFrame()
    for i in results:
        if i[1] == {}:
            error_results = error_results.append({"error_variables": i[0]}, ignore_index=True)

    combined_by_symbol = [pd.concat([s.T for t, s in m.items() if t != "ALL"], axis=0) for _, m in results if m.items()]

    combined_all = [pd.concat([s.T for t, s in m.items() if t == "ALL"], axis=0) for _, m in results if m.items()]

    try:
        symbols_summary = pd.concat(combined_by_symbol, axis=0).loc["total"]
        all_summary = pd.concat(combined_all, axis=0).loc["total"]

        filename = f"{strategy_name}_multi_summary_" f"{time.time()}.xlsx"

        excel_out_directory = create_directory("/backtest_result")
        excel_path = os.path.join(excel_out_directory, filename)

        with pd.ExcelWriter(excel_path) as writer:  # pylint: disable=abstract-class-instantiated
            all_summary.to_excel(writer, sheet_name="all")
            symbols_summary.to_excel(writer, sheet_name="symbols")
            error_results.to_excel(writer, sheet_name="errors")

        print(all_summary)
    except Exception:
        raise ValueError("No data to create summary.")

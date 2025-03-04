import os
import datetime
import json
from dataclasses import dataclass
from typing import List
import requests

from common.config import Config as config


@dataclass
class BacktestArgs:
    initial_usd: float
    commission: float
    use_analyze_per_dataframe: bool
    start_time: datetime.date = None
    end_time: datetime.date = None


@dataclass
class MultiOrderArgs:
    order_interval: int
    split_count: int


@dataclass
class Args:
    strategy: str
    nickname: str
    author: str
    ex_name: str
    ex_class: str
    symbols: List[str]
    interval: str
    history_days: int
    reset_variables: bool
    ex_alias: str = None
    leverage: int = None
    backtest: BacktestArgs = None
    fill_missing_data: bool = True
    multi_order: MultiOrderArgs = None


def __get_json(strategy_name: str):
    directory = os.path.dirname(os.path.realpath(__file__)) + "/../strategies/"
    with open(directory + strategy_name + ".json") as json_file:
        json_data = json.load(json_file)
        return json_data


def __get_live_json(strategy_name: str):
    json_config = config.json()
    url = f'http://{json_config["Url"]}:{json_config["Port"]}' f"/api/strategy/item/{strategy_name}"

    r = requests.get(url)
    json_data = r.json()
    return json_data


def create_args(strategy_name, is_live) -> Args:
    if is_live:
        # TODO sungmkim to get from live json
        #data = __get_live_json(strategy_name)
        data = __get_json(strategy_name)
    else:
        data = __get_json(strategy_name)

    args: Args = Args(
        strategy=data["strategy"],
        nickname=strategy_name,
        ex_name=data["exchange_name"],
        ex_class=data["exchange_class"],
        symbols=[x.upper() for x in data["symbols"]],
        interval=data["interval"],
        history_days=data["history_days"],
        reset_variables=data["reset_variables"],
        fill_missing_data=data["fill_missing_data"] if "fill_missing_data" in data else True,
        author=data["author"] if "author" in data else None,
    )

    if "exchange_alias" in data:
        args.ex_alias = data["exchange_alias"]

    #TODO sungmkim - CCXT or Argos_Order
    args.ex_alias = "sungmkim1980"
    args.author = "sungmkim1980"

    if is_live and not args.ex_alias:
        raise ValueError("ex_alias is missing in live mode")

    if "leverage" in data:
        args.leverage = data["leverage"]

    if not is_live:
        args.backtest = BacktestArgs(
            initial_usd=data["backtest"]["initial_usd"],
            commission=data["backtest"]["commission"],
            use_analyze_per_dataframe=data["backtest"]["use_analyze_per_dataframe"],
        )

        if "T" in data["backtest"]["start_time"]:
            args.backtest.start_time = datetime.datetime.strptime(data["backtest"]["start_time"], "%Y-%m-%dT%H:%M:%S")
        else:
            args.backtest.start_time = datetime.datetime.strptime(data["backtest"]["start_time"], "%Y-%m-%d")

        if "T" in data["backtest"]["end_time"]:
            args.backtest.end_time = datetime.datetime.strptime(
                data["backtest"]["end_time"], "%Y-%m-%dT%H:%M:%S"
            ) + datetime.timedelta(0, 1)
        else:
            args.backtest.end_time = datetime.datetime.strptime(
                data["backtest"]["end_time"], "%Y-%m-%d"
            ) + datetime.timedelta(0, 1)

    if "multi_order" in data:
        args.multi_order = MultiOrderArgs(
            order_interval=data["multi_order"]["order_interval"] or 1,
            split_count=data["multi_order"]["split_count"] or 3,
        )
    return args

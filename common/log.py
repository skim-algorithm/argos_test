import logging

min_log_level = logging.INFO
console_handler = logging.StreamHandler()


def makeLogger(strategy_name: str):
    logger = logging.getLogger(__name__)
    if len(logger.handlers) > 0:
        return logger

    logger.propagate = False
    logger.setLevel(min_log_level)
    log_format = logging.Formatter(f"%(asctime)s - {strategy_name} - %(levelname)s - %(message)s")
    console_handler.setLevel(min_log_level)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)
    return logger


def customLogger(strategy_name: str, logtime):
    logger = logging.getLogger(__name__)

    if len(logger.handlers) > 0:
        return logger

    logger.propagate = False
    logger.setLevel(logging.INFO)
    log_format = logging.Formatter(f"{logtime} - {strategy_name} - %(levelname)s - %(message)s")
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(log_format)
    logger.addHandler(console)
    return logger

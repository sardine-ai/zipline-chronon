import logging
import os
import time
from enum import Enum


class DataprocJobType(Enum):
    SPARK = "spark"
    FLINK = "flink"


def retry_decorator(retries=3, backoff=20):
    def wrapper(func):
        def wrapped(*args, **kwargs):
            attempt = 0
            while attempt <= retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    logging.exception(e)
                    sleep_time = attempt * backoff
                    logging.info(
                        "[{}] Retry: {} out of {}/ Sleeping for {}".format(
                            func.__name__, attempt, retries, sleep_time
                        )
                    )
                    time.sleep(sleep_time)
            return func(*args, **kwargs)

        return wrapped

    return wrapper


def get_environ_arg(env_name) -> str:
    value = os.environ.get(env_name)
    if not value:
        raise ValueError(f"Please set {env_name} environment variable")
    return value


def get_customer_id() -> str:
    return get_environ_arg('CUSTOMER_ID')


def extract_filename_from_path(path):
    return path.split("/")[-1]

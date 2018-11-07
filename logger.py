
import logging
from multiprocessing import Queue
from logging.handlers import QueueHandler, QueueListener


# --------------------------------------------------------------------------------------------------
def logger_worker_init(q):
    qh = QueueHandler(q)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(qh)


# --------------------------------------------------------------------------------------------------
def logger_init(log_fname):
    q = Queue()

    try:
        handler = logging.FileHandler(log_fname)
    except PermissionError as e:
        print("logger_init: Error = ", str(e))
        handler = logging.StreamHandler()
        print("logger_init: StreamHandler selected.")
    except Exception as e:
        print("logger_init: Unexpected Error = ", str(e))
        handler = logging.StreamHandler()

    handler.setFormatter(logging.Formatter("%(levelname)s: %(asctime)s - %(process)s - %(message)s"))

    ql = QueueListener(q, handler)
    ql.start()

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    return ql, q

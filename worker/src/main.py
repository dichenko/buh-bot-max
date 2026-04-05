from __future__ import annotations

import logging
import time


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def run_forever() -> None:
    logging.info("Python worker started")
    logging.info("Current mode: scaffold. Queue integration will be added in next iterations.")

    while True:
        logging.info("Worker heartbeat: alive")
        time.sleep(60)


if __name__ == "__main__":
    run_forever()
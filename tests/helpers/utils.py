import typing
import time
import logging


T_WAIT_TIME = typing.Union[int, float]


class Utils:
    @staticmethod
    def PrintAndSleep(wait: T_WAIT_TIME):
        assert type(wait) in [int, float]
        logging.info("Wait for {} second(s)".format(wait))
        time.sleep(wait)
        return

    @staticmethod
    def WaitUntil(
        error_message: str = "Did not complete",
        timeout: T_WAIT_TIME = 30,
        interval: T_WAIT_TIME = 1,
        notification_interval: T_WAIT_TIME = 5,
    ):
        """
        Loop until the timeout is reached. If the timeout is reached, raise an
        exception with the given error message.

        Source of idea: pgbouncer
        """
        assert type(timeout) in [int, float]
        assert type(interval) in [int, float]

        start_ts = time.monotonic()
        end_ts = start_ts + timeout
        last_printed_progress = start_ts
        last_iteration_ts = start_ts

        yield
        attempt = 1

        while end_ts > time.monotonic():
            if (timeout > 5 and time.monotonic() - last_printed_progress) > notification_interval:
                last_printed_progress = time.monotonic()

                m = "{} in {} seconds and {} attempts - will retry".format(
                    error_message,
                    time.monotonic() - start_ts,
                    attempt,
                )
                logging.info(m)

            interval_remaining = last_iteration_ts + interval - time.monotonic()
            if interval_remaining > 0:
                time.sleep(interval_remaining)

            last_iteration_ts = time.monotonic()
            yield
            attempt += 1
            continue

        raise TimeoutError(error_message + " in time")

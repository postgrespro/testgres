# //////////////////////////////////////////////////////////////////////////////
from __future__ import annotations

import typing
import logging
import time
import datetime
import dataclasses


# //////////////////////////////////////////////////////////////////////////////
# MultiTryCall


class MultiTryCall:
    T_SLEEP_SECONDS = typing.Union[int, float]

    # --------------------------------------------------------------------
    @dataclasses.dataclass
    class tagSETTINGS:
        T_EQUAL_COMPARER = typing.Callable[[typing.Any, typing.Any], bool]

        max_attempts: typing.Optional[int] = None
        sleep_seconds: MultiTryCall.T_SLEEP_SECONDS = 0.5
        suppressed_exceptions: typing.Optional[typing.List[type]] = None
        end_ts: typing.Optional[datetime.datetime] = None

        stable_count: typing.Optional[int] = None
        stable_tester: typing.Optional[T_EQUAL_COMPARER] = None

    # --------------------------------------------------------------------
    @staticmethod
    def exec(
        method: typing.Callable,
        operationDescr: str,
        settings: tagSETTINGS,
        *args,
        **kwargs
    ) -> typing.Any:
        assert isinstance(method, typing.Callable)
        assert type(operationDescr) is str
        assert type(settings) is __class__.tagSETTINGS
        assert type(settings.sleep_seconds) is int or type(settings.sleep_seconds) is float
        assert operationDescr != ""
        assert settings.max_attempts is None or settings.max_attempts > 0
        assert type(settings.sleep_seconds) in [int, float]
        assert float(settings.sleep_seconds) >= 0
        assert settings.suppressed_exceptions is None or type(settings.suppressed_exceptions) is list

        return __class__.helper__exec__until(
            method,
            __class__.helper__return_true,
            operationDescr,
            settings,
            *args,
            **kwargs
        )

    # --------------------------------------------------------------------
    @dataclasses.dataclass
    class tagTryResult:
        ok: bool
        value: typing.Any
        fail_reason: typing.Optional[str]
        last_exception: typing.Optional[BaseException]

    # --------------------------------------------------------------------
    @staticmethod
    def try_exec__test(
        testMethod: typing.Callable[..., bool],
        operationDescr: str,
        settings: tagSETTINGS,
        *args,
        **kwargs,
    ) -> tagTryResult:
        assert isinstance(testMethod, typing.Callable)
        assert type(operationDescr) is str
        assert type(settings) is __class__.tagSETTINGS
        assert type(settings.sleep_seconds) is int or type(settings.sleep_seconds) is float
        assert operationDescr != ""
        assert settings.max_attempts is None or settings.max_attempts > 0
        assert type(settings.sleep_seconds) in [int, float]
        assert float(settings.sleep_seconds) >= 0
        assert settings.suppressed_exceptions is None or type(settings.suppressed_exceptions) is list

        return __class__.helper__try_exec__until(
            testMethod,
            __class__.helper__is_true,
            operationDescr,
            settings,
            *args,
            **kwargs,
        )

    # --------------------------------------------------------------------
    @staticmethod
    def try_exec__until(
        execMethod: typing.Callable[..., typing.Any],
        testMethod: typing.Callable[[typing.Any], bool],
        operationDescr: str,
        settings: tagSETTINGS,
        *args,
        **kwargs,
    ) -> tagTryResult:
        assert isinstance(execMethod, typing.Callable)
        assert isinstance(testMethod, typing.Callable)
        assert type(operationDescr) is str
        assert type(settings) is __class__.tagSETTINGS
        assert type(settings.sleep_seconds) is int or type(settings.sleep_seconds) is float
        assert operationDescr != ""
        assert settings.max_attempts is None or settings.max_attempts > 0
        assert type(settings.sleep_seconds) in [int, float]
        assert float(settings.sleep_seconds) >= 0
        assert settings.suppressed_exceptions is None or type(settings.suppressed_exceptions) is list

        return __class__.helper__try_exec__until(
            execMethod,
            testMethod,
            operationDescr,
            settings,
            *args,
            **kwargs,
        )

    # --------------------------------------------------------------------
    @staticmethod
    def exec__test(
        testMethod: typing.Callable[..., bool],
        operationDescr: str,
        settings: tagSETTINGS,
        *args,
        **kwargs,
    ) -> None:
        assert isinstance(testMethod, typing.Callable)
        assert type(operationDescr) is str
        assert type(settings) is __class__.tagSETTINGS
        assert type(settings.sleep_seconds) is int or type(settings.sleep_seconds) is float
        assert operationDescr != ""
        assert settings.max_attempts is None or settings.max_attempts > 0
        assert type(settings.sleep_seconds) in [int, float]
        assert float(settings.sleep_seconds) >= 0
        assert settings.suppressed_exceptions is None or type(settings.suppressed_exceptions) is list

        __class__.helper__exec__until(
            testMethod,
            __class__.helper__is_true,
            operationDescr,
            settings,
            *args,
            **kwargs,
        )
        return

    # --------------------------------------------------------------------
    @staticmethod
    def exec__until(
        execMethod: typing.Callable[..., typing.Any],
        testMethod: typing.Callable[[typing.Any], bool],
        operationDescr: str,
        settings: tagSETTINGS,
        *args,
        **kwargs,
    ) -> typing.Any:
        assert isinstance(execMethod, typing.Callable)
        assert isinstance(testMethod, typing.Callable)
        assert type(operationDescr) is str
        assert type(settings) is __class__.tagSETTINGS
        assert type(settings.sleep_seconds) is int or type(settings.sleep_seconds) is float
        assert operationDescr != ""
        assert settings.max_attempts is None or settings.max_attempts > 0
        assert type(settings.sleep_seconds) in [int, float]
        assert float(settings.sleep_seconds) >= 0
        assert settings.suppressed_exceptions is None or type(settings.suppressed_exceptions) is list

        return __class__.helper__exec__until(
            execMethod,
            testMethod,
            operationDescr,
            settings,
            *args,
            **kwargs,
        )

    # --------------------------------------------------------------------
    @staticmethod
    def helper__is_true(
        value: bool,
    ) -> bool:
        assert type(value) is bool
        return value

    # --------------------------------------------------------------------
    @staticmethod
    def helper__exec__until(
        execMethod: typing.Callable[..., typing.Any],
        testMethod: typing.Callable[[typing.Any], bool],
        operationDescr: str,
        settings: tagSETTINGS,
        *args,
        **kwargs,
    ) -> typing.Any:
        r = __class__.helper__try_exec__until(
            execMethod,
            testMethod,
            operationDescr,
            settings,
            *args,
            **kwargs,
        )

        assert type(r) is __class__.tagTryResult

        if r.ok:
            return r.value

        assert type(r.fail_reason) is str
        raise RuntimeError(r.fail_reason) from r.last_exception

    # --------------------------------------------------------------------
    @staticmethod
    def helper__try_exec__until(
        execMethod: typing.Callable[..., typing.Any],
        testMethod: typing.Callable[[typing.Any], bool],
        operationDescr: str,
        settings: tagSETTINGS,
        *args,
        **kwargs,
    ) -> tagTryResult:
        assert isinstance(execMethod, typing.Callable)
        assert isinstance(testMethod, typing.Callable)
        assert type(operationDescr) is str
        assert type(settings) is __class__.tagSETTINGS
        assert type(settings.sleep_seconds) is int or type(settings.sleep_seconds) is float
        assert operationDescr != ""
        assert settings.max_attempts is None or settings.max_attempts > 0
        assert settings.sleep_seconds >= 0
        assert settings.suppressed_exceptions is None or type(settings.suppressed_exceptions) is list

        nAttempt = 0
        nStable = 0
        prev_result: typing.Any = None

        while True:
            assert settings.max_attempts is None or nAttempt < settings.max_attempts

            if settings.end_ts is not None:
                current_ts = datetime.datetime.now(tz=datetime.timezone.utc)
                if settings.end_ts < current_ts:
                    err_msg = "The operation [{}] is timeout. {} attempt(s) made.".format(
                        operationDescr,
                        nAttempt,
                    )
                    raise TimeoutError(err_msg)

            nAttempt += 1

            if nAttempt > 1:
                logging.info(
                    "Sleep [{0}] seconds before the next attempt to do [{1}] ...".format(
                        settings.sleep_seconds,
                        operationDescr,
                    )
                )

                time.sleep(settings.sleep_seconds)

            logging.info(
                "Try to do [{0}]. Attempt {1}/{2} ...".format(
                    operationDescr,
                    nAttempt,
                    settings.max_attempts if settings.max_attempts is not None else "None",
                )
            )

            try:
                r = execMethod(*args, **kwargs)
            except AssertionError:
                raise
            except BaseException as e:
                nStable = 0
                prev_result = None

                assert settings.max_attempts is None or nAttempt <= settings.max_attempts

                msg = __class__.Helper__build_op_exc_message(operationDescr, e)
                logging.info(msg)

                if not __class__.Helper__should_we_suppress_exception(
                    e,
                    settings.suppressed_exceptions,
                ):
                    raise

                if settings.max_attempts is None or nAttempt < settings.max_attempts:
                    continue

                assert nAttempt == settings.max_attempts

                logging.info("It was the last ({}) attempt. Exception will be reraised.".format(
                    nAttempt
                ))

                return __class__.tagTryResult(
                    ok=False,
                    value=None,
                    fail_reason="Operation [{0}] failed. {1} attempts were made.".format(
                        operationDescr,
                        nAttempt,
                    ),
                    last_exception=e,
                )

            assert nStable >= 0

            if settings.stable_tester is not None:
                assert isinstance(settings.stable_tester, typing.Callable)

                if nStable > 0 and not settings.stable_tester(prev_result, r):
                    nStable = 0

                prev_result = r
                nStable += 1

                assert settings.stable_count is not None and settings.stable_count >= 1

                if nStable < settings.stable_count:
                    logging.info("Operation [{}] got last_result {} time(s)".format(
                        operationDescr,
                        nStable,
                    ))
                    continue

            if testMethod(r):
                logging.info("Operation [{0}] is succeeded.".format(
                    operationDescr,
                ))
                return __class__.tagTryResult(
                    ok=True,
                    value=r,
                    fail_reason=None,
                    last_exception=None,
                )

            if settings.max_attempts is not None and nAttempt == settings.max_attempts:
                return __class__.tagTryResult(
                    ok=False,
                    value=None,
                    fail_reason="Operation [{0}] failed. {1} attempts were made.".format(
                        operationDescr,
                        nAttempt,
                    ),
                    last_exception=None,
                )

            logging.info("Operation [{0}] still in progress.".format(operationDescr))
            continue

    # --------------------------------------------------------------------
    @staticmethod
    def helper__return_true(v: typing.Any) -> bool:
        return True

    # Helper methods -----------------------------------------------------
    @staticmethod
    def Helper__should_we_suppress_exception(
            exc: BaseException,
            suppressed_exception: typing.Optional[typing.List[type]],
    ) -> bool:
        assert isinstance(exc, BaseException)
        assert suppressed_exception is None or type(suppressed_exception) is list

        if suppressed_exception is None:
            return True

        for t in suppressed_exception:
            assert t is not None
            assert type(t) is type
            assert issubclass(t, BaseException)

            if isinstance(exc, t):
                return True
            continue

        return False

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__build_op_exc_message(
        opDescr: str,
        exc: BaseException,
    ) -> str:
        assert type(opDescr) is str
        assert isinstance(exc, BaseException)

        if exc.__cause__ is None:
            msg = "Operation [{0}] raised the exception ({1}): {2}".format(
                opDescr, type(exc).__name__, exc
            )
            return msg

        msg = "Operation [{0}] raised the complex exception:".format(opDescr)

        prev = exc
        num = 0
        e2: typing.Optional[BaseException] = exc
        while e2 is not None:
            assert isinstance(e2, BaseException)
            assert prev is not None

            num += 1

            line = "{}. {} - {}".format(
                num,
                type(e2).__name__,
                str(e2).strip()
            )

            msg += "\n" + line

            e2 = e2.__cause__

            if (num % 2) == 0:
                assert prev.__cause__ is not None
                prev = prev.__cause__

            assert e2 is not prev
            continue

        assert num > 1
        return msg


# //////////////////////////////////////////////////////////////////////////////

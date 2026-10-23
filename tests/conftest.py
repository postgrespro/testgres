# /////////////////////////////////////////////////////////////////////////////
# PyTest Configuration

from .conftest_helpers import TestStartupData
from .conftest_helpers import TestServices
from .conftest_helpers import TestExitStatus

import pluggy
import pytest
import os
import logging
import pathlib
import math
import datetime
import typing
import enum
import threading

import _pytest.outcomes
import _pytest.logging

from packaging.version import Version

# /////////////////////////////////////////////////////////////////////////////

T_TUPLE__str_int = typing.Tuple[str, int]

# /////////////////////////////////////////////////////////////////////////////
# T_PLUGGY_RESULT

if Version(pluggy.__version__) <= Version("1.2"):
    T_PLUGGY_RESULT = pluggy._result._Result
else:
    T_PLUGGY_RESULT = pluggy.Result

# /////////////////////////////////////////////////////////////////////////////

g_error_msg_count_key = pytest.StashKey[int]()
g_warning_msg_count_key = pytest.StashKey[int]()
g_critical_msg_count_key = pytest.StashKey[int]()

# /////////////////////////////////////////////////////////////////////////////
# T_TEST_PROCESS_KIND


class T_TEST_PROCESS_KIND(enum.Enum):
    Master = 1
    Worker = 2


# /////////////////////////////////////////////////////////////////////////////
# T_TEST_PROCESS_MODE


class T_TEST_PROCESS_MODE(enum.Enum):
    Collect = 1
    ExecTests = 2


# /////////////////////////////////////////////////////////////////////////////
# TPDATA


class TPDATA:
    test_process_kind: typing.Optional[T_TEST_PROCESS_KIND] = None
    test_process_mode: typing.Optional[T_TEST_PROCESS_MODE] = None
    worker_log_is_created: typing.Optional[bool] = None

    cTotalTests: int = 0
    cNotExecutedTests: int = 0
    cExecutedTests: int = 0
    cPassedTests: int = 0
    cFailedTests: int = 0
    cXFailedTests: int = 0
    cSkippedTests: int = 0
    cNotXFailedTests: int = 0
    cWarningTests: int = 0
    cUnexpectedTests: int = 0
    cAchtungTests: int = 0

    FailedTests: typing.List[T_TUPLE__str_int] = list()
    XFailedTests: typing.List[T_TUPLE__str_int] = list()
    NotXFailedTests: typing.List[str] = list()
    WarningTests: typing.List[T_TUPLE__str_int] = list()
    AchtungTests: typing.List[str] = list()

    cTotalDuration: datetime.timedelta = datetime.timedelta()

    cTotalErrors: int = 0
    cTotalWarnings: int = 0

    # --------------------------------------------------------------------
    @staticmethod
    def incrementTotalTestCount() -> None:
        assert type(__class__.cTotalTests) is int
        assert __class__.cTotalTests >= 0

        __class__.cTotalTests += 1

        assert __class__.cTotalTests > 0
        return

    # --------------------------------------------------------------------
    @staticmethod
    def incrementNotExecutedTestCount() -> None:
        assert type(__class__.cNotExecutedTests) is int
        assert __class__.cNotExecutedTests >= 0

        __class__.cNotExecutedTests += 1

        assert __class__.cNotExecutedTests > 0
        return

    # --------------------------------------------------------------------
    @staticmethod
    def incrementExecutedTestCount() -> int:
        assert type(__class__.cExecutedTests) is int
        assert __class__.cExecutedTests >= 0

        __class__.cExecutedTests += 1

        assert __class__.cExecutedTests > 0
        return __class__.cExecutedTests

    # --------------------------------------------------------------------
    @staticmethod
    def incrementPassedTestCount() -> None:
        assert type(__class__.cPassedTests) is int
        assert __class__.cPassedTests >= 0

        __class__.cPassedTests += 1

        assert __class__.cPassedTests > 0
        return

    # --------------------------------------------------------------------
    @staticmethod
    def incrementFailedTestCount(testID: str, errCount: int) -> None:
        assert type(testID) is str
        assert type(errCount) is int
        assert errCount > 0
        assert type(__class__.FailedTests) is list
        assert type(__class__.cFailedTests) is int
        assert __class__.cFailedTests >= 0

        __class__.FailedTests.append((testID, errCount))  # raise?
        __class__.cFailedTests += 1

        assert len(__class__.FailedTests) > 0
        assert __class__.cFailedTests > 0
        assert len(__class__.FailedTests) == __class__.cFailedTests

        # --------
        assert type(__class__.cTotalErrors) is int
        assert __class__.cTotalErrors >= 0

        __class__.cTotalErrors += errCount

        assert __class__.cTotalErrors > 0
        return

    # --------------------------------------------------------------------
    @staticmethod
    def incrementXFailedTestCount(testID: str, errCount: int) -> None:
        assert type(testID) is str
        assert type(errCount) is int
        assert errCount >= 0
        assert type(__class__.XFailedTests) is list
        assert type(__class__.cXFailedTests) is int
        assert __class__.cXFailedTests >= 0

        __class__.XFailedTests.append((testID, errCount))  # raise?
        __class__.cXFailedTests += 1

        assert len(__class__.XFailedTests) > 0
        assert __class__.cXFailedTests > 0
        assert len(__class__.XFailedTests) == __class__.cXFailedTests
        return

    # --------------------------------------------------------------------
    @staticmethod
    def incrementSkippedTestCount() -> None:
        assert type(__class__.cSkippedTests) is int
        assert __class__.cSkippedTests >= 0

        __class__.cSkippedTests += 1

        assert __class__.cSkippedTests > 0
        return

    # --------------------------------------------------------------------
    @staticmethod
    def incrementNotXFailedTests(testID: str) -> None:
        assert type(testID) is str
        assert type(__class__.NotXFailedTests) is list
        assert type(__class__.cNotXFailedTests) is int
        assert __class__.cNotXFailedTests >= 0

        __class__.NotXFailedTests.append(testID)  # raise?
        __class__.cNotXFailedTests += 1

        assert len(__class__.NotXFailedTests) > 0
        assert __class__.cNotXFailedTests > 0
        assert len(__class__.NotXFailedTests) == __class__.cNotXFailedTests
        return

    # --------------------------------------------------------------------
    @staticmethod
    def incrementWarningTestCount(testID: str, warningCount: int) -> None:
        assert type(testID) is str
        assert type(warningCount) is int
        assert testID != ""
        assert warningCount > 0
        assert type(__class__.WarningTests) is list
        assert type(__class__.cWarningTests) is int
        assert __class__.cWarningTests >= 0

        __class__.WarningTests.append((testID, warningCount))  # raise?
        __class__.cWarningTests += 1

        assert len(__class__.WarningTests) > 0
        assert __class__.cWarningTests > 0
        assert len(__class__.WarningTests) == __class__.cWarningTests

        # --------
        assert type(__class__.cTotalWarnings) is int
        assert __class__.cTotalWarnings >= 0

        __class__.cTotalWarnings += warningCount

        assert __class__.cTotalWarnings > 0
        return

    # --------------------------------------------------------------------
    @staticmethod
    def incrementUnexpectedTests() -> None:
        assert type(__class__.cUnexpectedTests) is int
        assert __class__.cUnexpectedTests >= 0

        __class__.cUnexpectedTests += 1

        assert __class__.cUnexpectedTests > 0
        return

    # --------------------------------------------------------------------
    @staticmethod
    def incrementAchtungTestCount(testID: str) -> None:
        assert type(testID) is str
        assert type(__class__.AchtungTests) is list
        assert type(__class__.cAchtungTests) is int
        assert __class__.cAchtungTests >= 0

        __class__.AchtungTests.append(testID)  # raise?
        __class__.cAchtungTests += 1

        assert len(__class__.AchtungTests) > 0
        assert __class__.cAchtungTests > 0
        assert len(__class__.AchtungTests) == __class__.cAchtungTests
        return


# /////////////////////////////////////////////////////////////////////////////


def timedelta_to_human_text(delta: datetime.timedelta) -> str:
    assert isinstance(delta, datetime.timedelta)

    C_SECONDS_IN_MINUTE = 60
    C_SECONDS_IN_HOUR = 60 * C_SECONDS_IN_MINUTE

    v = delta.seconds

    cHours = int(v / C_SECONDS_IN_HOUR)
    v = v - cHours * C_SECONDS_IN_HOUR
    cMinutes = int(v / C_SECONDS_IN_MINUTE)
    cSeconds = v - cMinutes * C_SECONDS_IN_MINUTE

    result = "" if delta.days == 0 else "{0} day(s) ".format(delta.days)

    result = result + "{:02d}:{:02d}:{:02d}.{:06d}".format(
        cHours, cMinutes, cSeconds, delta.microseconds
    )

    return result


# /////////////////////////////////////////////////////////////////////////////


def helper__build_test_id(item: pytest.Function) -> str:
    assert item is not None
    assert isinstance(item, pytest.Function)

    testID = ""

    if item.cls is not None:
        testID = item.cls.__module__ + "." + item.cls.__name__ + "::"

    testID = testID + item.name

    return testID


# /////////////////////////////////////////////////////////////////////////////


def helper__exc_to_text(exc: BaseException) -> str:
    assert isinstance(exc, BaseException)
    return TestServices.ExceptionToHumanText(exc)


# /////////////////////////////////////////////////////////////////////////////


def helper__makereport__setup(
    item: pytest.Function, call: pytest.CallInfo, outcome: T_PLUGGY_RESULT
):
    assert item is not None
    assert call is not None
    assert outcome is not None
    # it may be pytest.Function or _pytest.unittest.TestCaseFunction
    assert isinstance(item, pytest.Function)
    assert type(call) is pytest.CallInfo
    assert type(outcome) is T_PLUGGY_RESULT

    C_LINE1 = "******************************************************"

    # logging.info("pytest_runtest_makereport - setup")

    TPDATA.incrementTotalTestCount()

    rep: pytest.TestReport = outcome.get_result()
    assert rep is not None
    assert type(rep) is pytest.TestReport

    if rep.outcome == "skipped":
        TPDATA.incrementNotExecutedTestCount()
        return

    testID = helper__build_test_id(item)

    if rep.outcome == "passed":
        testNumber = TPDATA.incrementExecutedTestCount()

        logging.info(C_LINE1)
        logging.info("* START TEST {0}".format(testID))
        logging.info("*")
        logging.info("* Path  : {0}".format(item.path))
        logging.info("* Number: {0}".format(testNumber))
        logging.info("*")
        return

    assert rep.outcome != "passed"

    TPDATA.incrementAchtungTestCount(testID)

    logging.info(C_LINE1)
    logging.info("* ACHTUNG TEST {0}".format(testID))
    logging.info("*")
    logging.info("* Path  : {0}".format(item.path))
    logging.info("* Outcome is [{0}]".format(rep.outcome))

    if rep.outcome == "failed":
        assert call.excinfo is not None
        assert call.excinfo.value is not None
        logging.info("*")
        logging.error(helper__exc_to_text(call.excinfo.value))

    logging.info("*")
    return


# ------------------------------------------------------------------------
def helper__makereport__call(
    item: pytest.Function, call: pytest.CallInfo, outcome: T_PLUGGY_RESULT
):
    assert item is not None
    assert call is not None
    assert outcome is not None
    # it may be pytest.Function or _pytest.unittest.TestCaseFunction
    assert isinstance(item, pytest.Function)
    assert type(call) is pytest.CallInfo
    assert type(outcome) is T_PLUGGY_RESULT

    # --------
    item_error_msg_count1 = item.stash.get(g_error_msg_count_key, 0)
    assert type(item_error_msg_count1) is int
    assert item_error_msg_count1 >= 0

    item_error_msg_count2 = item.stash.get(g_critical_msg_count_key, 0)
    assert type(item_error_msg_count2) is int
    assert item_error_msg_count2 >= 0

    item_error_msg_count = item_error_msg_count1 + item_error_msg_count2

    # --------
    item_warning_msg_count = item.stash.get(g_warning_msg_count_key, 0)
    assert type(item_warning_msg_count) is int
    assert item_warning_msg_count >= 0

    # --------
    rep = outcome.get_result()
    assert rep is not None
    assert type(rep) is pytest.TestReport

    # --------
    testID = helper__build_test_id(item)

    # --------
    assert call.start <= call.stop

    startDT = datetime.datetime.fromtimestamp(call.start)
    assert type(startDT) is datetime.datetime
    stopDT = datetime.datetime.fromtimestamp(call.stop)
    assert type(stopDT) is datetime.datetime

    testDurration = stopDT - startDT
    assert type(testDurration) is datetime.timedelta

    # --------
    exitStatus: typing.Optional[TestExitStatus] = None
    exitStatusInfo = None
    if rep.outcome == "skipped":
        assert call.excinfo is not None  # research
        assert call.excinfo.value is not None  # research

        if type(call.excinfo.value) is _pytest.outcomes.Skipped:
            assert not hasattr(rep, "wasxfail")

            exitStatus = TestExitStatus.SKIPPED
            reasonText = str(call.excinfo.value)
            reasonMsgTempl = "SKIP REASON: {0}"

            TPDATA.incrementSkippedTestCount()

        elif type(call.excinfo.value) is _pytest.outcomes.XFailed:
            exitStatus = TestExitStatus.XFAILED
            reasonText = str(call.excinfo.value)
            reasonMsgTempl = "XFAIL REASON: {0}"

            TPDATA.incrementXFailedTestCount(testID, item_error_msg_count)

        else:
            exitStatus = TestExitStatus.XFAILED
            assert hasattr(rep, "wasxfail")
            assert rep.wasxfail is not None
            assert type(rep.wasxfail) is str

            reasonText = rep.wasxfail
            reasonMsgTempl = "XFAIL REASON: {0}"

            if type(call.excinfo.value) is SIGNAL_EXCEPTION:
                pass
            else:
                logging.error(helper__exc_to_text(call.excinfo.value))
                item_error_msg_count += 1

            TPDATA.incrementXFailedTestCount(testID, item_error_msg_count)

        assert type(reasonText) is str

        if reasonText != "":
            assert type(reasonMsgTempl) is str
            logging.info("*")
            logging.info("* " + reasonMsgTempl.format(reasonText))

    elif rep.outcome == "failed":
        assert call.excinfo is not None
        assert call.excinfo.value is not None

        if type(call.excinfo.value) is SIGNAL_EXCEPTION:
            assert item_error_msg_count > 0
            pass
        else:
            logging.error(helper__exc_to_text(call.excinfo.value))
            item_error_msg_count += 1

        assert item_error_msg_count > 0
        TPDATA.incrementFailedTestCount(testID, item_error_msg_count)

        exitStatus = TestExitStatus.FAILED
    elif rep.outcome == "passed":
        assert call.excinfo is None

        if hasattr(rep, "wasxfail"):
            assert type(rep.wasxfail) is str

            TPDATA.incrementNotXFailedTests(testID)

            warnMsg = "NOTE: Test is marked as xfail"

            if rep.wasxfail != "":
                warnMsg += " [" + rep.wasxfail + "]"

            logging.info(warnMsg)
            exitStatus = TestExitStatus.NOT_XFAILED
        else:
            assert not hasattr(rep, "wasxfail")

            TPDATA.incrementPassedTestCount()
            exitStatus = TestExitStatus.PASSED
    else:
        TPDATA.incrementUnexpectedTests()
        exitStatus = TestExitStatus.UNEXPECTED
        exitStatusInfo = rep.outcome
        # [2025-03-28] It may create a useless problem in new environment.
        # assert False

    # --------
    if item_warning_msg_count > 0:
        TPDATA.incrementWarningTestCount(testID, item_warning_msg_count)

    # --------
    assert exitStatus is not None
    assert type(exitStatus) is TestExitStatus

    assert exitStatus != TestExitStatus.FAILED or item_error_msg_count > 0

    TestServices.CleanTestTmpDirBeforeExit(
        item,
        exitStatus,
    )

    # --------
    assert type(TPDATA.cTotalDuration) is datetime.timedelta
    assert type(testDurration) is datetime.timedelta

    TPDATA.cTotalDuration += testDurration

    assert testDurration <= TPDATA.cTotalDuration

    # --------
    exitStatusLineData = exitStatus.value

    if exitStatusInfo is not None:
        exitStatusLineData += " [{}]".format(exitStatusInfo)

    # --------
    logging.info("*")
    logging.info("* DURATION     : {0}".format(timedelta_to_human_text(testDurration)))
    logging.info("*")
    logging.info("* EXIT STATUS  : {0}".format(exitStatusLineData))
    logging.info("* ERROR COUNT  : {0}".format(item_error_msg_count))
    logging.info("* WARNING COUNT: {0}".format(item_warning_msg_count))
    logging.info("*")
    logging.info("* STOP TEST {0}".format(testID))
    logging.info("*")
    return


# /////////////////////////////////////////////////////////////////////////////


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Function, call: pytest.CallInfo):
    #
    # https://docs.pytest.org/en/7.1.x/how-to/writing_hook_functions.html#hookwrapper-executing-around-other-hooks
    #
    # Note that hook wrappers don’t return results themselves,
    # they merely perform tracing or other side effects around the actual hook implementations.
    #
    # https://docs.pytest.org/en/7.1.x/reference/reference.html#test-running-runtest-hooks
    #
    assert item is not None
    assert call is not None
    # it may be pytest.Function or _pytest.unittest.TestCaseFunction
    assert isinstance(item, pytest.Function)
    assert type(call) is pytest.CallInfo

    outcome = yield
    assert outcome is not None
    assert type(outcome) is T_PLUGGY_RESULT

    assert type(call.when) is str

    if call.when == "collect":
        return

    if call.when == "setup":
        helper__makereport__setup(item, call, outcome)
        return

    if call.when == "call":
        helper__makereport__call(item, call, outcome)
        return

    if call.when == "teardown":
        return

    errMsg = "[pytest_runtest_makereport] unknown 'call.when' value: [{0}].".format(
        call.when
    )

    raise RuntimeError(errMsg)


# /////////////////////////////////////////////////////////////////////////////


class LogWrapper2:
    _guard: threading.Lock
    _old_method: typing.Any
    _err_counter: typing.Optional[int]
    _warn_counter: typing.Optional[int]

    _critical_counter: typing.Optional[int]

    # --------------------------------------------------------------------
    def __init__(self):
        self._guard = threading.Lock()
        self._old_method = None
        self._err_counter = None
        self._warn_counter = None

        self._critical_counter = None
        return

    # --------------------------------------------------------------------
    def __enter__(self):
        assert self._guard is not None
        # assert isinstance(self._guard, threading.Lock)
        assert self._old_method is None
        assert self._err_counter is None
        assert self._warn_counter is None

        assert self._critical_counter is None

        assert logging.root is not None
        assert isinstance(logging.root, logging.RootLogger)

        self._old_method = logging.root.handle
        self._err_counter = 0
        self._warn_counter = 0

        self._critical_counter = 0

        logging.root.handle = self
        return self

    # --------------------------------------------------------------------
    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self._guard is not None
        # assert isinstance(self._guard, threading.Lock)
        assert self._old_method is not None
        assert self._err_counter is not None
        assert self._warn_counter is not None

        assert logging.root is not None
        assert isinstance(logging.root, logging.RootLogger)

        assert logging.root.handle is self

        logging.root.handle = self._old_method

        self._old_method = None
        self._err_counter = None
        self._warn_counter = None
        self._critical_counter = None
        return False

    # --------------------------------------------------------------------
    def __call__(self, record: logging.LogRecord):
        assert record is not None
        assert isinstance(record, logging.LogRecord)
        assert self._guard is not None
        # assert isinstance(self._guard, threading.Lock)
        assert self._old_method is not None
        assert self._err_counter is not None
        assert self._warn_counter is not None
        assert self._critical_counter is not None

        assert type(self._err_counter) is int
        assert self._err_counter >= 0
        assert type(self._warn_counter) is int
        assert self._warn_counter >= 0
        assert type(self._critical_counter) is int
        assert self._critical_counter >= 0

        r = self._old_method(record)

        with self._guard:
            if record.levelno == logging.ERROR:
                self._err_counter += 1
                assert self._err_counter > 0
            elif record.levelno == logging.WARNING:
                self._warn_counter += 1
                assert self._warn_counter > 0
            elif record.levelno == logging.CRITICAL:
                self._critical_counter += 1
                assert self._critical_counter > 0

        return r


# /////////////////////////////////////////////////////////////////////////////


class SIGNAL_EXCEPTION(Exception):
    def __init__(self):
        pass


# /////////////////////////////////////////////////////////////////////////////


@pytest.hookimpl(hookwrapper=True)
def pytest_pyfunc_call(pyfuncitem: pytest.Function):
    assert pyfuncitem is not None
    assert isinstance(pyfuncitem, pytest.Function)

    assert logging.root is not None
    assert isinstance(logging.root, logging.RootLogger)
    assert logging.root.handle is not None

    debug__log_handle_method = logging.root.handle
    assert debug__log_handle_method is not None

    debug__log_error_method = logging.error
    assert debug__log_error_method is not None

    debug__log_warning_method = logging.warning
    assert debug__log_warning_method is not None

    pyfuncitem.stash[g_error_msg_count_key] = 0
    pyfuncitem.stash[g_warning_msg_count_key] = 0
    pyfuncitem.stash[g_critical_msg_count_key] = 0

    try:
        with LogWrapper2() as logWrapper:
            assert type(logWrapper) is LogWrapper2
            assert logWrapper._old_method is not None
            assert type(logWrapper._err_counter) is int
            assert logWrapper._err_counter == 0
            assert type(logWrapper._warn_counter) is int
            assert logWrapper._warn_counter == 0
            assert type(logWrapper._critical_counter) is int
            assert logWrapper._critical_counter == 0
            assert logging.root.handle is logWrapper

            r = yield

            assert r is not None
            assert type(r) is T_PLUGGY_RESULT

            assert logWrapper._old_method is not None
            assert type(logWrapper._err_counter) is int
            assert logWrapper._err_counter >= 0
            assert type(logWrapper._warn_counter) is int
            assert logWrapper._warn_counter >= 0
            assert type(logWrapper._critical_counter) is int
            assert logWrapper._critical_counter >= 0
            assert logging.root.handle is logWrapper

            assert g_error_msg_count_key in pyfuncitem.stash
            assert g_warning_msg_count_key in pyfuncitem.stash
            assert g_critical_msg_count_key in pyfuncitem.stash

            assert pyfuncitem.stash[g_error_msg_count_key] == 0
            assert pyfuncitem.stash[g_warning_msg_count_key] == 0
            assert pyfuncitem.stash[g_critical_msg_count_key] == 0

            pyfuncitem.stash[g_error_msg_count_key] = logWrapper._err_counter
            pyfuncitem.stash[g_warning_msg_count_key] = logWrapper._warn_counter
            pyfuncitem.stash[g_critical_msg_count_key] = logWrapper._critical_counter

            if r.exception is not None:
                pass
            elif logWrapper._err_counter > 0:
                r.force_exception(SIGNAL_EXCEPTION())
            elif logWrapper._critical_counter > 0:
                r.force_exception(SIGNAL_EXCEPTION())
    finally:
        assert logging.error is debug__log_error_method
        assert logging.warning is debug__log_warning_method
        assert logging.root.handle == debug__log_handle_method
        pass


# /////////////////////////////////////////////////////////////////////////////


def helper__calc_W(n: int) -> int:
    assert n > 0

    x = int(math.log10(n))
    assert type(x) is int
    assert x >= 0
    x += 1
    return x


# ------------------------------------------------------------------------
def helper__print_test_list(tests: typing.List[str]) -> None:
    assert type(tests) is list

    assert helper__calc_W(9) == 1
    assert helper__calc_W(10) == 2
    assert helper__calc_W(11) == 2
    assert helper__calc_W(99) == 2
    assert helper__calc_W(100) == 3
    assert helper__calc_W(101) == 3
    assert helper__calc_W(999) == 3
    assert helper__calc_W(1000) == 4
    assert helper__calc_W(1001) == 4

    W = helper__calc_W(len(tests))

    templateLine = "{0:0" + str(W) + "d}. {1}"

    nTest = 0

    for t in tests:
        assert type(t) is str
        assert t != ""
        nTest += 1
        logging.info(templateLine.format(nTest, t))


# ------------------------------------------------------------------------
def helper__print_test_list2(tests: typing.List[T_TUPLE__str_int]) -> None:
    assert type(tests) is list

    assert helper__calc_W(9) == 1
    assert helper__calc_W(10) == 2
    assert helper__calc_W(11) == 2
    assert helper__calc_W(99) == 2
    assert helper__calc_W(100) == 3
    assert helper__calc_W(101) == 3
    assert helper__calc_W(999) == 3
    assert helper__calc_W(1000) == 4
    assert helper__calc_W(1001) == 4

    W = helper__calc_W(len(tests))

    templateLine = "{0:0" + str(W) + "d}. {1} ({2})"

    nTest = 0

    for t in tests:
        assert type(t) is tuple
        assert len(t) == 2
        assert type(t[0]) is str
        assert type(t[1]) is int
        assert t[0] != ""
        assert t[1] >= 0
        nTest += 1
        logging.info(templateLine.format(nTest, t[0], t[1]))


# /////////////////////////////////////////////////////////////////////////////
# SUMMARY BUILDER


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish():
    #
    # NOTE: It should execute after logging.pytest_sessionfinish
    #

    assert TPDATA.test_process_kind is not None
    assert type(TPDATA.test_process_kind) is T_TEST_PROCESS_KIND

    if TPDATA.test_process_kind == T_TEST_PROCESS_KIND.Master:
        return

    assert TPDATA.test_process_kind == T_TEST_PROCESS_KIND.Worker

    assert TPDATA.test_process_mode is not None
    assert type(TPDATA.test_process_mode) is T_TEST_PROCESS_MODE

    if TPDATA.test_process_mode == T_TEST_PROCESS_MODE.Collect:
        return

    assert TPDATA.test_process_mode == T_TEST_PROCESS_MODE.ExecTests

    assert type(TPDATA.worker_log_is_created) is bool
    assert TPDATA.worker_log_is_created

    C_LINE1 = "---------------------------"

    def LOCAL__print_line1_with_header(header: str):
        assert type(C_LINE1) is str
        assert type(header) is str
        assert header != ""
        logging.info(C_LINE1 + " [" + header + "]")

    def LOCAL__print_test_list(
        header: str, test_count: int, test_list: typing.List[str]
    ):
        assert type(header) is str
        assert type(test_count) is int
        assert type(test_list) is list
        assert header != ""
        assert test_count >= 0
        assert len(test_list) == test_count

        LOCAL__print_line1_with_header(header)
        logging.info("")
        if len(test_list) > 0:
            helper__print_test_list(test_list)
            logging.info("")

    def LOCAL__print_test_list2(
        header: str, test_count: int, test_list: typing.List[T_TUPLE__str_int]
    ):
        assert type(header) is str
        assert type(test_count) is int
        assert type(test_list) is list
        assert header != ""
        assert test_count >= 0
        assert len(test_list) == test_count

        LOCAL__print_line1_with_header(header)
        logging.info("")
        if len(test_list) > 0:
            helper__print_test_list2(test_list)
            logging.info("")

    # fmt: off
    LOCAL__print_test_list(
        "ACHTUNG TESTS",
        TPDATA.cAchtungTests,
        TPDATA.AchtungTests,
    )

    LOCAL__print_test_list2(
        "FAILED TESTS",
        TPDATA.cFailedTests,
        TPDATA.FailedTests
    )

    LOCAL__print_test_list2(
        "XFAILED TESTS",
        TPDATA.cXFailedTests,
        TPDATA.XFailedTests,
    )

    LOCAL__print_test_list(
        "NOT XFAILED TESTS",
        TPDATA.cNotXFailedTests,
        TPDATA.NotXFailedTests,
    )

    LOCAL__print_test_list2(
        "WARNING TESTS",
        TPDATA.cWarningTests,
        TPDATA.WarningTests,
    )
    # fmt: on

    LOCAL__print_line1_with_header("SUMMARY STATISTICS")
    logging.info("")
    logging.info("[TESTS]")
    logging.info(" TOTAL        : {0}".format(TPDATA.cTotalTests))
    logging.info(" EXECUTED     : {0}".format(TPDATA.cExecutedTests))
    logging.info(" NOT EXECUTED : {0}".format(TPDATA.cNotExecutedTests))
    logging.info(" ACHTUNG      : {0}".format(TPDATA.cAchtungTests))
    logging.info("")
    logging.info(" PASSED       : {0}".format(TPDATA.cPassedTests))
    logging.info(" FAILED       : {0}".format(TPDATA.cFailedTests))
    logging.info(" XFAILED      : {0}".format(TPDATA.cXFailedTests))
    logging.info(" NOT XFAILED  : {0}".format(TPDATA.cNotXFailedTests))
    logging.info(" SKIPPED      : {0}".format(TPDATA.cSkippedTests))
    logging.info(" WITH WARNINGS: {0}".format(TPDATA.cWarningTests))
    logging.info(" UNEXPECTED   : {0}".format(TPDATA.cUnexpectedTests))
    logging.info("")

    assert type(TPDATA.cTotalDuration) is datetime.timedelta

    LOCAL__print_line1_with_header("TIME")
    logging.info("")
    logging.info(
        " TOTAL DURATION: {0}".format(
            timedelta_to_human_text(TPDATA.cTotalDuration)
        )
    )
    logging.info("")

    LOCAL__print_line1_with_header("TOTAL INFORMATION")
    logging.info("")
    logging.info(" TOTAL ERROR COUNT  : {0}".format(TPDATA.cTotalErrors))
    logging.info(" TOTAL WARNING COUNT: {0}".format(TPDATA.cTotalWarnings))
    logging.info("")


# /////////////////////////////////////////////////////////////////////////////


def helper__detect_test_process_kind(config: pytest.Config) -> T_TEST_PROCESS_KIND:
    assert isinstance(config, pytest.Config)

    #
    # xdist' master process registers DSession plugin.
    #
    p = config.pluginmanager.get_plugin("dsession")

    if p is not None:
        return T_TEST_PROCESS_KIND.Master

    return T_TEST_PROCESS_KIND.Worker


# ------------------------------------------------------------------------
def helper__detect_test_process_mode(config: pytest.Config) -> T_TEST_PROCESS_MODE:
    assert isinstance(config, pytest.Config)

    if config.getvalue("collectonly"):
        return T_TEST_PROCESS_MODE.Collect

    return T_TEST_PROCESS_MODE.ExecTests


# ------------------------------------------------------------------------
@pytest.hookimpl(trylast=True)
def helper__pytest_configure__logging(config: pytest.Config) -> None:
    assert isinstance(config, pytest.Config)

    log_name = TestStartupData.GetCurrentTestWorkerSignature()
    log_name += ".log"

    log_dir = TestStartupData.GetRootLogDir()

    pathlib.Path(log_dir).mkdir(exist_ok=True)

    logging_plugin = config.pluginmanager.get_plugin(
        "logging-plugin",
    )

    assert logging_plugin is not None
    assert isinstance(logging_plugin, _pytest.logging.LoggingPlugin)

    log_file_path = os.path.join(log_dir, log_name)
    assert log_file_path is not None
    assert type(log_file_path) is str

    logging_plugin.set_log_path(log_file_path)
    return


# ------------------------------------------------------------------------
@pytest.hookimpl(trylast=True)
def pytest_configure(config: pytest.Config) -> None:
    assert isinstance(config, pytest.Config)

    assert TPDATA.test_process_kind is None
    assert TPDATA.test_process_mode is None
    assert TPDATA.worker_log_is_created is None

    TPDATA.test_process_mode = helper__detect_test_process_mode(config)
    TPDATA.test_process_kind = helper__detect_test_process_kind(config)

    assert type(TPDATA.test_process_kind) is T_TEST_PROCESS_KIND
    assert type(TPDATA.test_process_mode) is T_TEST_PROCESS_MODE

    if TPDATA.test_process_kind == T_TEST_PROCESS_KIND.Master:
        pass
    else:
        assert TPDATA.test_process_kind == T_TEST_PROCESS_KIND.Worker

        if TPDATA.test_process_mode == T_TEST_PROCESS_MODE.Collect:
            TPDATA.worker_log_is_created = False
        else:
            assert TPDATA.test_process_mode == T_TEST_PROCESS_MODE.ExecTests
            helper__pytest_configure__logging(config)
            TPDATA.worker_log_is_created = True

    return


# /////////////////////////////////////////////////////////////////////////////

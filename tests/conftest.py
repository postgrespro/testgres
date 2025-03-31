# /////////////////////////////////////////////////////////////////////////////
# PyTest Configuration

import pluggy
import pytest
import os
import logging
import pathlib
import math
import datetime

import _pytest.outcomes
import _pytest.unittest
import _pytest.logging

# /////////////////////////////////////////////////////////////////////////////

C_ROOT_DIR__RELATIVE = ".."

# /////////////////////////////////////////////////////////////////////////////
# TestConfigPropNames


class TestConfigPropNames:
    TEST_CFG__LOG_DIR = "TEST_CFG__LOG_DIR"


# /////////////////////////////////////////////////////////////////////////////
# TestStartupData__Helper


class TestStartupData__Helper:
    sm_StartTS = datetime.datetime.now()

    # --------------------------------------------------------------------
    def GetStartTS() -> datetime.datetime:
        assert type(__class__.sm_StartTS) == datetime.datetime  # noqa: E721
        return __class__.sm_StartTS

    # --------------------------------------------------------------------
    def CalcRootDir() -> str:
        r = os.path.abspath(__file__)
        r = os.path.dirname(r)
        r = os.path.join(r, C_ROOT_DIR__RELATIVE)
        r = os.path.abspath(r)
        return r

    # --------------------------------------------------------------------
    def CalcCurrentTestWorkerSignature() -> str:
        currentPID = os.getpid()
        assert type(currentPID)

        startTS = __class__.sm_StartTS
        assert type(startTS)

        result = "pytest-{0:04d}{1:02d}{2:02d}_{3:02d}{4:02d}{5:02d}".format(
            startTS.year,
            startTS.month,
            startTS.day,
            startTS.hour,
            startTS.minute,
            startTS.second,
        )

        gwid = os.environ.get("PYTEST_XDIST_WORKER")

        if gwid is not None:
            result += "--xdist_" + str(gwid)

        result += "--" + "pid" + str(currentPID)
        return result


# /////////////////////////////////////////////////////////////////////////////
# TestStartupData


class TestStartupData:
    sm_RootDir: str = TestStartupData__Helper.CalcRootDir()
    sm_CurrentTestWorkerSignature: str = (
        TestStartupData__Helper.CalcCurrentTestWorkerSignature()
    )

    # --------------------------------------------------------------------
    def GetRootDir() -> str:
        assert type(__class__.sm_RootDir) == str  # noqa: E721
        return __class__.sm_RootDir

    # --------------------------------------------------------------------
    def GetCurrentTestWorkerSignature() -> str:
        assert type(__class__.sm_CurrentTestWorkerSignature) == str  # noqa: E721
        return __class__.sm_CurrentTestWorkerSignature


# /////////////////////////////////////////////////////////////////////////////
# TEST_PROCESS_STATS


class TEST_PROCESS_STATS:
    cTotalTests: int = 0
    cNotExecutedTests: int = 0
    cExecutedTests: int = 0
    cPassedTests: int = 0
    cFailedTests: int = 0
    cXFailedTests: int = 0
    cSkippedTests: int = 0
    cNotXFailedTests: int = 0
    cUnexpectedTests: int = 0
    cAchtungTests: int = 0

    FailedTests = list[str]()
    XFailedTests = list[str]()
    NotXFailedTests = list[str]()
    AchtungTests = list[str]()

    # --------------------------------------------------------------------
    def incrementTotalTestCount() -> None:
        __class__.cTotalTests += 1

    # --------------------------------------------------------------------
    def incrementNotExecutedTestCount() -> None:
        __class__.cNotExecutedTests += 1

    # --------------------------------------------------------------------
    def incrementExecutedTestCount() -> int:
        __class__.cExecutedTests += 1
        return __class__.cExecutedTests

    # --------------------------------------------------------------------
    def incrementPassedTestCount() -> None:
        __class__.cPassedTests += 1

    # --------------------------------------------------------------------
    def incrementFailedTestCount(testID: str) -> None:
        assert type(testID) == str  # noqa: E721
        assert type(__class__.FailedTests) == list  # noqa: E721

        __class__.FailedTests.append(testID)  # raise?
        __class__.cFailedTests += 1

    # --------------------------------------------------------------------
    def incrementXFailedTestCount(testID: str) -> None:
        assert type(testID) == str  # noqa: E721
        assert type(__class__.XFailedTests) == list  # noqa: E721

        __class__.XFailedTests.append(testID)  # raise?
        __class__.cXFailedTests += 1

    # --------------------------------------------------------------------
    def incrementSkippedTestCount() -> None:
        __class__.cSkippedTests += 1

    # --------------------------------------------------------------------
    def incrementNotXFailedTests(testID: str) -> None:
        assert type(testID) == str  # noqa: E721
        assert type(__class__.NotXFailedTests) == list  # noqa: E721

        __class__.NotXFailedTests.append(testID)  # raise?
        __class__.cNotXFailedTests += 1

    # --------------------------------------------------------------------
    def incrementUnexpectedTests() -> None:
        __class__.cUnexpectedTests += 1

    # --------------------------------------------------------------------
    def incrementAchtungTestCount(testID: str) -> None:
        assert type(testID) == str  # noqa: E721
        assert type(__class__.AchtungTests) == list  # noqa: E721

        __class__.AchtungTests.append(testID)  # raise?
        __class__.cAchtungTests += 1


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


def helper__makereport__setup(
    item: pytest.Function, call: pytest.CallInfo, outcome: pluggy.Result
):
    assert item is not None
    assert call is not None
    assert outcome is not None
    # it may be pytest.Function or _pytest.unittest.TestCaseFunction
    assert isinstance(item, pytest.Function)
    assert type(call) == pytest.CallInfo  # noqa: E721
    assert type(outcome) == pluggy.Result  # noqa: E721

    C_LINE1 = "******************************************************"

    # logging.info("pytest_runtest_makereport - setup")

    TEST_PROCESS_STATS.incrementTotalTestCount()

    rep: pytest.TestReport = outcome.get_result()
    assert rep is not None
    assert type(rep) == pytest.TestReport  # noqa: E721

    if rep.outcome == "skipped":
        TEST_PROCESS_STATS.incrementNotExecutedTestCount()
        return

    testID = helper__build_test_id(item)

    if rep.outcome == "passed":
        testNumber = TEST_PROCESS_STATS.incrementExecutedTestCount()

        logging.info(C_LINE1)
        logging.info("* START TEST {0}".format(testID))
        logging.info("*")
        logging.info("* Path  : {0}".format(item.path))
        logging.info("* Number: {0}".format(testNumber))
        logging.info("*")
        return

    assert rep.outcome != "passed"

    TEST_PROCESS_STATS.incrementAchtungTestCount(testID)

    logging.info(C_LINE1)
    logging.info("* ACHTUNG TEST {0}".format(testID))
    logging.info("*")
    logging.info("* Path  : {0}".format(item.path))
    logging.info("* Outcome is [{0}]".format(rep.outcome))

    if rep.outcome == "failed":
        assert call.excinfo is not None
        assert call.excinfo.value is not None
        logging.info("*")
        logging.error(call.excinfo.value)

    logging.info("*")
    return


# ------------------------------------------------------------------------
def helper__makereport__call(
    item: pytest.Function, call: pytest.CallInfo, outcome: pluggy.Result
):
    assert item is not None
    assert call is not None
    assert outcome is not None
    # it may be pytest.Function or _pytest.unittest.TestCaseFunction
    assert isinstance(item, pytest.Function)
    assert type(call) == pytest.CallInfo  # noqa: E721
    assert type(outcome) == pluggy.Result  # noqa: E721

    rep = outcome.get_result()
    assert rep is not None
    assert type(rep) == pytest.TestReport  # noqa: E721

    # --------
    testID = helper__build_test_id(item)

    # --------
    assert call.start <= call.stop

    startDT = datetime.datetime.fromtimestamp(call.start)
    assert type(startDT) == datetime.datetime  # noqa: E721
    stopDT = datetime.datetime.fromtimestamp(call.stop)
    assert type(stopDT) == datetime.datetime  # noqa: E721

    testDurration = stopDT - startDT
    assert type(testDurration) == datetime.timedelta  # noqa: E721

    # --------
    exitStatus = None
    if rep.outcome == "skipped":
        assert call.excinfo is not None  # research
        assert call.excinfo.value is not None  # research

        if type(call.excinfo.value) == _pytest.outcomes.Skipped:  # noqa: E721
            assert not hasattr(rep, "wasxfail")

            TEST_PROCESS_STATS.incrementSkippedTestCount()

            exitStatus = "SKIPPED"
            reasonText = str(call.excinfo.value)
            reasonMsg = "SKIP REASON: {0}"

        elif type(call.excinfo.value) == _pytest.outcomes.XFailed:  # noqa: E721
            TEST_PROCESS_STATS.incrementXFailedTestCount(testID)

            exitStatus = "XFAILED"
            reasonText = str(call.excinfo.value)
            reasonMsg = "XFAIL REASON: {0}"
        else:
            exitStatus = "XFAILED"
            assert hasattr(rep, "wasxfail")
            assert rep.wasxfail is not None
            assert type(rep.wasxfail) == str  # noqa: E721

            TEST_PROCESS_STATS.incrementXFailedTestCount(testID)

            reasonText = rep.wasxfail
            reasonMsg = "XFAIL REASON: {0}"

            logging.error(call.excinfo.value)

        if reasonText != "":
            logging.info("*")
            logging.info("* " + reasonMsg.format(reasonText))

    elif rep.outcome == "failed":
        assert call.excinfo is not None
        assert call.excinfo.value is not None

        TEST_PROCESS_STATS.incrementFailedTestCount(testID)

        logging.error(call.excinfo.value)
        exitStatus = "FAILED"
    elif rep.outcome == "passed":
        assert call.excinfo is None

        if hasattr(rep, "wasxfail"):
            assert type(rep.wasxfail) == str  # noqa: E721

            TEST_PROCESS_STATS.incrementNotXFailedTests(testID)

            warnMsg = "Test is marked as xfail"

            if rep.wasxfail != "":
                warnMsg += " [" + rep.wasxfail + "]"

            logging.warning(warnMsg)
            exitStatus = "NOT XFAILED"
        else:
            assert not hasattr(rep, "wasxfail")

            TEST_PROCESS_STATS.incrementPassedTestCount()
            exitStatus = "PASSED"
    else:
        TEST_PROCESS_STATS.incrementUnexpectedTests()
        exitStatus = "UNEXPECTED [{0}]".format(rep.outcome)
        # [2025-03-28] It may create a useless problem in new environment.
        # assert False

    # --------
    logging.info("*")
    logging.info("* DURATION    : {0}".format(timedelta_to_human_text(testDurration)))
    logging.info("*")
    logging.info("* EXIT STATUS : {0}".format(exitStatus))
    logging.info("*")
    logging.info("* STOP TEST {0}".format(testID))
    logging.info("*")


# /////////////////////////////////////////////////////////////////////////////


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Function, call: pytest.CallInfo):
    assert item is not None
    assert call is not None
    # it may be pytest.Function or _pytest.unittest.TestCaseFunction
    assert isinstance(item, pytest.Function)
    assert type(call) == pytest.CallInfo  # noqa: E721

    outcome: pluggy.Result = yield
    assert outcome is not None
    assert type(outcome) == pluggy.Result  # noqa: E721

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

    assert False


# /////////////////////////////////////////////////////////////////////////////


def helper__calc_W(n: int) -> int:
    assert n > 0

    x = int(math.log10(n))
    assert type(x) == int  # noqa: E721
    assert x >= 0
    x += 1
    return x


# ------------------------------------------------------------------------
def helper__print_test_list(tests: list[str]) -> None:
    assert type(tests) == list  # noqa: E721

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

    while nTest < len(tests):
        testID = tests[nTest]
        assert type(testID) == str  # noqa: E721
        nTest += 1
        logging.info(templateLine.format(nTest, testID))


# /////////////////////////////////////////////////////////////////////////////


@pytest.fixture(autouse=True, scope="session")
def run_after_tests(request: pytest.FixtureRequest):
    assert isinstance(request, pytest.FixtureRequest)

    yield

    C_LINE1 = "---------------------------"

    def LOCAL__print_line1_with_header(header: str):
        assert type(C_LINE1) == str  # noqa: E721
        assert type(header) == str  # noqa: E721
        assert header != ""
        logging.info(C_LINE1 + " [" + header + "]")

    def LOCAL__print_test_list(header: str, test_count: int, test_list: list[str]):
        assert type(header) == str  # noqa: E721
        assert type(test_count) == int  # noqa: E721
        assert type(test_list) == list  # noqa: E721
        assert header != ""
        assert test_count >= 0
        assert len(test_list) == test_count

        LOCAL__print_line1_with_header(header)
        logging.info("")
        if len(test_list) > 0:
            helper__print_test_list(test_list)
            logging.info("")

    # fmt: off
    LOCAL__print_test_list(
        "ACHTUNG TESTS",
        TEST_PROCESS_STATS.cAchtungTests,
        TEST_PROCESS_STATS.AchtungTests,
    )

    LOCAL__print_test_list(
        "FAILED TESTS",
        TEST_PROCESS_STATS.cFailedTests,
        TEST_PROCESS_STATS.FailedTests
    )

    LOCAL__print_test_list(
        "XFAILED TESTS",
        TEST_PROCESS_STATS.cXFailedTests,
        TEST_PROCESS_STATS.XFailedTests,
    )

    LOCAL__print_test_list(
        "NOT XFAILED TESTS",
        TEST_PROCESS_STATS.cNotXFailedTests,
        TEST_PROCESS_STATS.NotXFailedTests,
    )
    # fmt: on

    LOCAL__print_line1_with_header("SUMMARY STATISTICS")
    logging.info("")
    logging.info("[TESTS]")
    logging.info(" TOTAL       : {0}".format(TEST_PROCESS_STATS.cTotalTests))
    logging.info(" EXECUTED    : {0}".format(TEST_PROCESS_STATS.cExecutedTests))
    logging.info(" NOT EXECUTED: {0}".format(TEST_PROCESS_STATS.cNotExecutedTests))
    logging.info(" ACHTUNG     : {0}".format(TEST_PROCESS_STATS.cAchtungTests))
    logging.info("")
    logging.info(" PASSED      : {0}".format(TEST_PROCESS_STATS.cPassedTests))
    logging.info(" FAILED      : {0}".format(TEST_PROCESS_STATS.cFailedTests))
    logging.info(" XFAILED     : {0}".format(TEST_PROCESS_STATS.cXFailedTests))
    logging.info(" NOT XFAILED : {0}".format(TEST_PROCESS_STATS.cNotXFailedTests))
    logging.info(" SKIPPED     : {0}".format(TEST_PROCESS_STATS.cSkippedTests))
    logging.info(" UNEXPECTED  : {0}".format(TEST_PROCESS_STATS.cUnexpectedTests))
    logging.info("")


# /////////////////////////////////////////////////////////////////////////////


@pytest.hookimpl(trylast=True)
def pytest_configure(config: pytest.Config) -> None:
    assert isinstance(config, pytest.Config)

    log_name = TestStartupData.GetCurrentTestWorkerSignature()
    log_name += ".log"

    if TestConfigPropNames.TEST_CFG__LOG_DIR in os.environ:
        log_path_v = os.environ[TestConfigPropNames.TEST_CFG__LOG_DIR]
        log_path = pathlib.Path(log_path_v)
    else:
        log_path = config.rootpath.joinpath("logs")

    log_path.mkdir(exist_ok=True)

    logging_plugin: _pytest.logging.LoggingPlugin = config.pluginmanager.get_plugin(
        "logging-plugin"
    )

    assert logging_plugin is not None
    assert isinstance(logging_plugin, _pytest.logging.LoggingPlugin)

    logging_plugin.set_log_path(str(log_path / log_name))


# /////////////////////////////////////////////////////////////////////////////

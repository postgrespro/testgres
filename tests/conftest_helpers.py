# /////////////////////////////////////////////////////////////////////////////
# PyTest Configuration Helpers

from __future__ import annotations

import os
import pytest
import logging
import time
import traceback
import typing
import shutil
import enum
import fnmatch
import datetime


# /////////////////////////////////////////////////////////////////////////////

C_ROOT_DIR__RELATIVE = ".."


# /////////////////////////////////////////////////////////////////////////////
# TestExitStatus

class TestExitStatus(enum.Enum):
    FAILED = "FAILED"
    PASSED = "PASSED"
    XFAILED = "XFAILED"
    NOT_XFAILED = "NOT XFAILED"
    SKIPPED = "SKIPPED"
    UNEXPECTED = "UNEXPECTED"


# /////////////////////////////////////////////////////////////////////////////
# TestConfigPropNames


class TestConfigPropNames:
    TEST_CFG__NO_CLEANUP = "TEST_CFG__NO_CLEANUP"

    TEST_CFG__TEMP_DIR = "TEST_CFG__TEMP_DIR"

    TEST_CFG__LOG_DIR = "TEST_CFG__LOG_DIR"

    TEST_CFG__ENABLE_XFAIL = "TEST_CFG__ENABLE_XFAIL"


# /////////////////////////////////////////////////////////////////////////////
# ThrowError


class ThrowError:
    @staticmethod
    def EnvVarIsNotDefined(envVarName: str) -> typing.NoReturn:
        assert type(envVarName) is str
        raise RuntimeError("System env variable [{}] is not defined.".format(
            envVarName,
        ))

    # --------------------------------------------------------------------
    @staticmethod
    def EnvVarHasBadValue(envVarName: str) -> typing.NoReturn:
        assert type(envVarName) is str
        raise RuntimeError("System env variable [{}] has bad value.".format(
            envVarName,
        ))

    # --------------------------------------------------------------------
    @staticmethod
    def EnvVarHasBadValue2(envVarName: str, envVarValue) -> typing.NoReturn:
        assert type(envVarName) is str
        raise RuntimeError("System env variable [{}] has bad value [{}].".format(
            envVarName,
            envVarValue,
        ))

# /////////////////////////////////////////////////////////////////////////////
# TestConfigHelper


class TestConfigHelper:
    @staticmethod
    def NoCleanup(
        exit_status: typing.Optional[TestExitStatus],
    ) -> bool:
        assert exit_status is None or type(exit_status) is TestExitStatus

        v = os.environ.get(TestConfigPropNames.TEST_CFG__NO_CLEANUP)

        if v is None:
            return False

        vv = str(v).upper()

        if vv in __class__.sm_NO:
            return False

        if vv in __class__.sm_YES:
            return True

        if exit_status is None:
            return False

        v2 = vv.split(",")

        if exit_status.name in v2:
            return True

        return False

    # --------------------------------------------------------------------
    @staticmethod
    def EnableXFail() -> bool:
        if TestConfigPropNames.TEST_CFG__ENABLE_XFAIL not in os.environ.keys():
            return False

        v = os.environ[TestConfigPropNames.TEST_CFG__ENABLE_XFAIL]

        return __class__.Helper__ToBoolean(v, TestConfigPropNames.TEST_CFG__ENABLE_XFAIL)

    # --------------------------------------------------------------------
    @staticmethod
    def GetEnvValue__STR(envName: str) -> typing.Optional[str]:
        assert type(envName) is str
        return os.getenv(envName)

    # --------------------------------------------------------------------
    @staticmethod
    def GetReqEnvValue__STR(envName: str) -> str:
        assert type(envName) is str

        v = os.getenv(envName)

        if v is None:
            ThrowError.EnvVarIsNotDefined(envName)
            assert False

        return v

    # Helper methods -----------------------------------------------------
    sm_YES: list[str] = ["1", "TRUE", "YES", "ON"]

    sm_NO: list[str] = ["0", "FALSE", "NO", "OFF"]

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__ToBoolean(v, envVarName: str) -> bool:
        assert type(envVarName) is str

        typeV = type(v)

        if typeV is bool:
            return v

        if typeV is str:
            vv = str(v).upper()
            assert type(vv) is str

            if vv in __class__.sm_YES:
                return True

            if vv in __class__.sm_NO:
                return False

            ThrowError.EnvVarHasBadValue2(envVarName, vv)
            return False

        if typeV is int:
            if v == 0:
                return False

            if v == 1:
                return True

            ThrowError.EnvVarHasBadValue2(envVarName, v)
            return False

        ThrowError.EnvVarHasBadValue(envVarName)
        return False

# /////////////////////////////////////////////////////////////////////////////
# TestStartupData__Helper


class TestStartupData__Helper:
    sm_StartTS = datetime.datetime.now()

    # --------------------------------------------------------------------
    @staticmethod
    def GetStartTS() -> datetime.datetime:
        assert type(__class__.sm_StartTS) is datetime.datetime
        return __class__.sm_StartTS

    # --------------------------------------------------------------------
    @staticmethod
    def CalcRootDir() -> str:
        r = os.path.abspath(__file__)
        r = os.path.dirname(r)
        r = os.path.join(r, C_ROOT_DIR__RELATIVE)
        r = os.path.abspath(r)
        return r

    # --------------------------------------------------------------------
    @staticmethod
    def CalcRootTmpDir() -> str:
        if TestConfigPropNames.TEST_CFG__TEMP_DIR in os.environ:
            resultPath = os.environ[TestConfigPropNames.TEST_CFG__TEMP_DIR]
        else:
            rootDir = __class__.CalcRootDir()
            resultPath = os.path.join(rootDir, "tmp")

        assert type(resultPath) is str
        return resultPath

    # --------------------------------------------------------------------
    @staticmethod
    def CalcRootLogDir() -> str:
        if TestConfigPropNames.TEST_CFG__LOG_DIR in os.environ:
            resultPath = os.environ[TestConfigPropNames.TEST_CFG__LOG_DIR]
        else:
            rootDir = __class__.CalcRootDir()
            resultPath = os.path.join(rootDir, "logs")

        assert type(resultPath) is str
        return resultPath

    # --------------------------------------------------------------------
    @staticmethod
    def CalcCurrentTestWorkerSignature() -> str:
        currentPID = os.getpid()
        assert type(currentPID) is int

        startTS = __class__.sm_StartTS
        assert type(startTS) is datetime.datetime

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
    sm_RootTmpDir: str = TestStartupData__Helper.CalcRootTmpDir()
    sm_RootTmpDataDir: str = os.path.join(sm_RootTmpDir, "data")
    sm_CurrentTestWorkerSignature: str = (
        TestStartupData__Helper.CalcCurrentTestWorkerSignature()
    )
    sm_RootTmpDataDirForCurrentTestWorker: str = os.path.join(
        sm_RootTmpDataDir, sm_CurrentTestWorkerSignature
    )

    sm_RootLogDir: str = TestStartupData__Helper.CalcRootLogDir()

    # --------------------------------------------------------------------
    @staticmethod
    def GetRootDir() -> str:
        assert type(__class__.sm_RootDir) is str
        return __class__.sm_RootDir

    # --------------------------------------------------------------------
    @staticmethod
    def GetRootLogDir() -> str:
        assert type(__class__.sm_RootLogDir) is str
        return __class__.sm_RootLogDir

    # --------------------------------------------------------------------
    @staticmethod
    def GetCurrentTestWorkerSignature() -> str:
        assert type(__class__.sm_CurrentTestWorkerSignature) is str
        return __class__.sm_CurrentTestWorkerSignature

    # --------------------------------------------------------------------
    @staticmethod
    def GetRootTmpDataDirForCurrentTestWorker() -> str:
        assert type(__class__.sm_RootTmpDataDirForCurrentTestWorker) is str
        return __class__.sm_RootTmpDataDirForCurrentTestWorker


# /////////////////////////////////////////////////////////////////////////////
# class TestTempDirCleaner2


class TestTempDirCleaner2:
    @staticmethod
    def exec(
        root_path: str,
        artifact_patterns: typing.Iterable[str],
    ):
        assert type(root_path) is str
        assert isinstance(artifact_patterns, typing.Iterable)

        stack: typing.List[__class__.tagStackItem] = []

        __class__.Helper__push(stack, root_path)

        while len(stack) > 0:
            head = stack[-1]
            assert type(head) is __class__.tagStackItem

            if len(head.data[1]) > 0:
                __class__.Helper__push(
                    stack,
                    os.path.join(head.path, head.data[1].pop()),
                )
                continue

            stack.pop()

            # delete files
            cNotDeleted = 0
            for f in head.data[2]:
                assert type(f) is str
                assert f != ""

                full_file_path = os.path.join(head.path, f)

                # Получаем путь относительно корня очистки для проверки масок (например: "pg_wal/0001.history")
                # Это нужно, чтобы работали маски вида "**/pg_wal/*.history"
                rel_path = os.path.relpath(full_file_path, root_path)

                # Проверяем, подходит ли файл под какую-либо маску артефактов
                is_artifact = False
                for pattern in artifact_patterns:
                    # fnmatch отлично понимает структуры с '/' и '*'
                    if fnmatch.fnmatch(rel_path, pattern) or fnmatch.fnmatch(f, pattern):
                        is_artifact = True
                        break

                if is_artifact:
                    cNotDeleted += 1
                    continue

                if not __class__.Helper__safe_delete_file(full_file_path):
                    cNotDeleted += 1
                continue

            if cNotDeleted > 0:
                continue

            x = __class__.Helper__walk(head.path)

            if x is None:
                # ACHTUNG
                continue

            if len(x[1]) > 0:
                continue

            if len(x[2]) > 0:
                continue

            __class__.Helper__safe_delete_dir(head.path)
            continue
        return

    # --------------------------------------------------------------------
    T_WALK_RESULT = typing.Tuple[
        str,
        typing.List[str],
        typing.List[str],
    ]

    # --------------------------------------------------------------------
    class tagStackItem:
        path: str
        data: TestTempDirCleaner2.T_WALK_RESULT

        def __init__(self, path, data):
            assert type(data) is tuple
            assert len(data) == 3
            assert type(data[0]) is str
            assert type(data[1]) is list  # dirs
            assert type(data[2]) is list  # files

            self.path = path
            self.data = data
            return

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__walk(path: str) -> typing.Optional[T_WALK_RESULT]:
        try:
            x = os.walk(path).__next__()
        except StopIteration:
            return None

        assert x is not None
        assert type(x) is tuple
        assert len(x) == 3
        assert type(x[0]) is str
        assert type(x[1]) is list  # dirs
        assert type(x[2]) is list  # files
        return x

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__push(
        stack: typing.List[tagStackItem],
        path: str,
    ) -> bool:
        assert type(path) is str
        assert path != ""
        x = __class__.Helper__walk(path)
        if x is None:
            # ACHTUNG
            return False
        stack.append(__class__.tagStackItem(path, x))
        return True

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__safe_delete_file(path: str) -> bool:
        assert type(path) is str
        assert path != ""
        try:
            os.remove(path)
        except Exception as e:
            msg = "File [{}] is not deleted. Reason ({}): {}".format(
                path,
                type(e).__name__,
                e,
            )
            logging.info(msg)
            return False
        return True

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__safe_delete_dir(path: str) -> bool:
        assert type(path) is str
        assert path != ""
        try:
            os.rmdir(path)
        except Exception as e:
            msg = "Dir [{}] is not deleted. Reason ({}): {}".format(
                path,
                type(e).__name__,
                e,
            )
            logging.info(msg)
            return False
        return True

# /////////////////////////////////////////////////////////////////////////////
# TestServices


class TestServices:
    C_UNPACKED_TMP_DIR_SIZE_TRESHOLD = 5 * 1024 * 1024

    # --------------------------------------------------------------------
    @staticmethod
    def GetRootDir() -> str:
        return TestStartupData.GetRootDir()

    # --------------------------------------------------------------------
    @staticmethod
    def GetRootTmpDir() -> str:
        return TestStartupData.GetRootTmpDataDirForCurrentTestWorker()

    # --------------------------------------------------------------------
    @staticmethod
    def MakeRootTmpDirForGlobalResources(globalResourceID: str) -> str:
        assert isinstance(globalResourceID, str)
        return os.path.join(__class__.GetRootTmpDir(), ".global", globalResourceID)

    # --------------------------------------------------------------------
    @staticmethod
    def GetCurTestTmpDir(request: pytest.FixtureRequest) -> str:
        assert isinstance(request, pytest.FixtureRequest)
        return __class__.Helper__GetCurTestTmpDir(request.node)

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__GetCurTestTmpDir(function: pytest.Function) -> str:
        assert isinstance(function, pytest.Function)

        rootDir = TestServices.GetRootDir()
        rootTmpDir = TestServices.GetRootTmpDir()

        # [2024-12-18] It is not a fact now.
        # assert rootTmpDir.startswith(rootDir)

        testPath = str(function.path)

        if not testPath.startswith(rootDir):
            raise Exception(
                "Root dir {0} is not found in testPath {1}.".format(rootDir, testPath)
            )

        testPath2 = testPath[len(rootDir) + 1:]

        result = os.path.join(rootTmpDir, testPath2)

        if function.cls is not None:
            clsName = function.cls.__name__
            result = os.path.join(result, clsName)

        result = os.path.join(result, function.name)

        return result

    # --------------------------------------------------------------------
    sm_ArtifactRules = {
        "**/*.log",
        "**/*.conf",
    }

    # --------------------------------------------------------------------
    @staticmethod
    def CleanTestTmpDirBeforeExit(
        function: pytest.Function,
        exit_status: TestExitStatus,
    ):
        assert isinstance(function, pytest.Function)
        assert type(exit_status) is TestExitStatus

        tmpDir = __class__.Helper__GetCurTestTmpDir(function)
        assert type(tmpDir) is str

        if not os.path.exists(tmpDir):
            return

        if TestConfigHelper.NoCleanup(exit_status):
            logging.info("A final data cleanup is disabled [test exit status is {}].".format(
                exit_status.name,
            ))
        else:
            logging.info("Tmp directory [{}] is cleaned...".format(
                tmpDir,
            ))

            TestTempDirCleaner2.exec(
                tmpDir,
                __class__.sm_ArtifactRules,
            )

            if not os.path.exists(tmpDir):
                return

        tmpDirSize = __class__.Helper__GetFolderSize(tmpDir)

        if tmpDirSize < __class__.C_UNPACKED_TMP_DIR_SIZE_TRESHOLD:
            return

        logging.info("Tmp directory [{}] will be archived [size: {}]...".format(
            tmpDir,
            __class__.Helper__FormatSize(tmpDirSize),
        ))

        shutil.make_archive(
            tmpDir,
            'zip',
            tmpDir,
        )

        shutil.rmtree(tmpDir)
        return

    # --------------------------------------------------------------------
    @staticmethod
    def CleanDirBeforeExit(dir_path: str):
        assert type(dir_path) is str

        if not os.path.exists(dir_path):
            return

        if TestConfigHelper.NoCleanup(None):
            logging.info("A final data cleanup is disabled.")
            return

        logging.info("Directory [{0}] is cleaned...".format(dir_path))

        TestTempDirCleaner2.exec(dir_path, __class__.sm_ArtifactRules)
        return

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__FormatSize(bytes_size: int) -> str:
        assert type(bytes_size) is int
        assert bytes_size >= 0

        bytes_size_f = float(bytes_size)
        for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
            if bytes_size_f < 1024:
                return __class__.Helper__FormatSizeBuilder(bytes_size_f, unit)
            bytes_size_f /= 1024
            continue
        return __class__.Helper__FormatSizeBuilder(bytes_size_f, "PB")

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__FormatSizeBuilder(
        bytes_size_f: float,
        unit: str,
    ) -> str:
        assert type(bytes_size_f) is float
        assert bytes_size_f >= 0
        assert type(unit) is str

        return "{:.2f} {}".format(
            bytes_size_f,
            unit,
        )

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__GetFolderSize(folder_path: str) -> int:
        total_size = 0

        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                # Пропускаем символические ссылки, чтобы избежать ошибок и зацикливания
                if not os.path.islink(file_path):
                    total_size += os.path.getsize(file_path)
                continue
            continue

        return total_size

    # --------------------------------------------------------------------
    @staticmethod
    def PrintExceptionOK(e: Exception):
        assert isinstance(e, Exception)

        logging.info(
            "OK. We catch an exception. {}".format(
                __class__.ExceptionToHumanText(e),
            )
        )
        return

    # --------------------------------------------------------------------
    @staticmethod
    def ThrowWeWaitAnException() -> typing.NoReturn:
        raise Exception("We wait an exception!")

    # --------------------------------------------------------------------
    @staticmethod
    def ThrowWeWaitAnXFailPleaseUpdateTest() -> typing.NoReturn:
        raise Exception("We wait an xfail, please update test!")

    # --------------------------------------------------------------------
    @staticmethod
    def LogCurrentExceptionAndThrowXFailItIsAnExpectedFailure() -> typing.NoReturn:
        logging.exception("We catch an expected problem.")
        raise pytest.xfail("It is an expected failure.")

    # --------------------------------------------------------------------
    @staticmethod
    def SleepWithPrint(
        sleepTimeInSec: float,
        message: typing.Optional[str] = None,
    ):
        assert message is None or type(message) is str

        prefix = ""

        if message is not None and message != "":
            prefix = message

            if not prefix.endswith("."):
                prefix += "."

            prefix += " "

        logging.info("{}Sleep {} second(s).".format(
            prefix,
            sleepTimeInSec,
        ))
        time.sleep(sleepTimeInSec)
        return

    # --------------------------------------------------------------------
    @staticmethod
    def ExceptionToHumanText(exc: BaseException) -> str:
        assert isinstance(exc, BaseException)

        if __class__.Helper__GetPrevExc(exc) is None:
            return __class__.Helper__ExceptionToHumanText__Single(exc)

        return __class__.Helper__ExceptionToHumanText__Chain(exc)

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__ExceptionToHumanText__Single(exc: BaseException) -> str:
        assert isinstance(exc, BaseException)

        if isinstance(exc, AssertionError):
            err_msg = "Exception ({}).".format(type(exc).__name__)
            assert type(err_msg) is str

            exc_info_lines = traceback.format_exception(exc)
            assert type(exc_info_lines) is list
            err_msg2 = "".join(exc_info_lines).strip()

            if err_msg2 != "":
                err_msg += " " + err_msg2

            assert type(err_msg) is str
            return err_msg

        err_msg = "Exception ({})".format(type(exc).__name__)
        assert type(err_msg) is str

        err_msg2 = str(exc).strip()

        if err_msg2 == "":
            err_msg += "."
        else:
            err_msg += ": " + err_msg2

        assert type(err_msg) is str
        return err_msg

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__ExceptionToHumanText__Chain(exc: BaseException) -> str:
        assert isinstance(exc, BaseException)

        chain: typing.List[BaseException] = []
        curr = exc

        processed_exc_ids: typing.Set[int] = set()

        while curr is not None:
            assert isinstance(curr, BaseException)
            if id(curr) in processed_exc_ids:
                logging.error("Cycle in exception chain: {}".format(
                    " --> ".join(
                        [type(x).__name__ for x in reversed(chain)],
                    )
                ))
                break

            processed_exc_ids.add(id(curr))

            chain.append(curr)
            curr = __class__.Helper__GetPrevExc(curr)
            continue

        lines: typing.List[str] = []

        lines.append("It is a chain of exceptions (len: {}):".format(
            len(chain),
        ))

        n = 0
        for e in reversed(chain):
            n += 1
            line1 = "---- {}. Exception ({})".format(n, type(e).__name__)
            line2 = __class__.Helper__GetExcMsg(e)

            if line2 == "":
                lines.append(line1)
            else:
                lines.append(line1 + ":")
                lines.append(line2)
            continue

        lines.append("--------")

        return "\n".join(lines)

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__GetPrevExc(exc: BaseException) -> typing.Optional[BaseException]:
        assert isinstance(exc, BaseException)
        return exc.__cause__ or exc.__context__

    # --------------------------------------------------------------------
    @staticmethod
    def Helper__GetExcMsg(exc: BaseException) -> str:
        assert isinstance(exc, BaseException)

        if isinstance(exc, AssertionError):
            exc_info_lines = traceback.format_exception(exc)
            assert type(exc_info_lines) is list
            return "".join(exc_info_lines).strip()

        return str(exc)

# /////////////////////////////////////////////////////////////////////////////

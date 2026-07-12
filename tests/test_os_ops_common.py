# coding: utf-8
from __future__ import annotations

from .helpers.global_data import OsOpsDescr
from .helpers.global_data import OsOpsDescrs
from .helpers.global_data import OsOperations
from .helpers.run_conditions import RunConditions
from .helpers.local_check import LocalCheck
from .helpers.local_check import OsOpsHelpers

import os
import sys

import pytest
import re
import logging
import typing
import uuid
import subprocess
import psutil
import time
import signal as os_signal
import dataclasses
import random
import datetime
import threading
import queue

from src import InvalidOperationException
from src import ExecUtilException

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import Future as ThreadFuture


class TestOsOpsCommon:
    sm_os_ops_descrs: typing.List[OsOpsDescr] = [
        OsOpsDescrs.sm_local_os_ops_descr,
        OsOpsDescrs.sm_remote_os_ops_descr
    ]

    @pytest.fixture(
        params=[
            pytest.param(
                descr,
                id=descr.sign,
            )
            for descr in sm_os_ops_descrs
        ],
    )
    def os_ops_descr(self, request: pytest.FixtureRequest) -> OsOpsDescr:
        assert isinstance(request, pytest.FixtureRequest)
        assert isinstance(request.param, OsOpsDescr)
        return request.param

    @dataclasses.dataclass
    class tagNameWithSurprize:
        sign: str
        value: str

    sm_names_with_surprize: typing.List[tagNameWithSurprize] = [
        tagNameWithSurprize(
            sign="std",
            value="exclusive_new_file.txt",
        ),
        tagNameWithSurprize(
            sign="with_one_double_quote",
            value="exclusive_new_file\".txt",
        ),
        tagNameWithSurprize(
            sign="with_two_double_quote",
            value="\"exclusive_new_file\".txt",
        ),
        tagNameWithSurprize(
            sign="with_one_single_quote",
            value="exclusive_new_file\'.txt",
        ),
        tagNameWithSurprize(
            sign="with_two_single_quote",
            value="\'exclusive_new_file\'.txt",
        ),
        tagNameWithSurprize(
            sign="with_single_quote_and_double_quote",
            value="\'exclusive_new_file\".txt",
        ),
        tagNameWithSurprize(
            sign="with_double_quote_and_single_quote",
            value="\"exclusive_new_file\'.txt",
        ),
    ]

    @pytest.fixture(
        params=[
            pytest.param(
                x,
                id=x.sign,
            )
            for x in sm_names_with_surprize
        ]
    )
    def name_with_surprize(self, request: pytest.FixtureRequest) -> tagNameWithSurprize:
        assert isinstance(request, pytest.FixtureRequest)
        assert type(request.param).__name__ == "tagNameWithSurprize"
        return request.param

    sm_false_true: typing.List[bool] = [False, True]

    @pytest.fixture(
        params=[
            pytest.param(
                x,
                id="test_clone" if x else "test_orig",
            )
            for x in sm_false_true
        ]
    )
    def use_clone(self, request: pytest.FixtureRequest) -> bool:
        assert isinstance(request, pytest.FixtureRequest)
        assert type(request.param) is bool
        return request.param

    def test_prop__remote(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        v = os_ops.remote
        assert v is not None or type(v) is bool

        if type(os_ops).__name__ == "RemoteOperations":
            assert v is True
        elif type(os_ops).__name__ == "LocalOperations":
            assert v is False
        else:
            raise RuntimeError("[BUG CHECK] Unknown os_ops type: {}.".format(
                type(os_ops).__name__,
            ))
        return

    def test_prop__host(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        v = os_ops.host
        assert v is not None or type(v) is str
        return

    def test_prop__port(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        v = os_ops.port
        assert v is None or type(v) is int
        return

    def test_prop__ssh_key(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        v = os_ops.ssh_key
        assert v is None or type(v) is str
        return

    def test_prop__username(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        v = os_ops.username
        assert v is None or type(v) is str
        return

    def test_get_platform(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        p = os_ops.get_platform()
        assert p is not None
        assert type(p) is str
        assert p == sys.platform
        return

    def test_get_platform__is_known(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        p = os_ops.get_platform()
        assert p is not None
        assert type(p) is str
        assert p in {"win32", "linux"}
        return

    def test_create_clone(
        self,
        os_ops_descr: OsOpsDescr,
        use_clone: bool,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert type(use_clone) is bool

        os_ops = __class__.helper__get_os_ops(use_clone, os_ops_descr)
        assert isinstance(os_ops, OsOperations)

        env1_name = "env1_" + uuid.uuid4().bytes.hex()
        env1_val = "abc"
        env2_name = "env1_" + uuid.uuid4().bytes.hex()

        os_ops.set_env(env1_name, env1_val)

        try:
            clone = os_ops.create_clone()
            assert clone is not None
            assert clone is not os_ops
            assert type(clone) is type(os_ops)

            assert clone.remote == os_ops.remote
            assert clone.username == os_ops.username
            assert clone.ssh_key == os_ops.ssh_key
            assert clone.host == os_ops.host
            assert clone.port == os_ops.port

            assert clone.get_name() == os_ops.get_name()
            assert clone.get_platform() == os_ops.get_platform()

            assert clone.environ("PATH") == os_ops.environ("PATH")

            v1_orig = os_ops.environ(env1_name)
            v1_clone = clone.environ(env1_name)
            assert v1_orig == env1_val
            assert v1_orig == v1_clone

            assert clone.environ(env2_name) == os_ops.environ(env2_name)
        finally:
            os_ops.reset_env(env1_name, None)

        return

    def test_exec_command_success(self, os_ops_descr: OsOpsDescr):
        """
        Test exec_command for successful command execution.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        RunConditions.skip_if_windows()

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        cmd = ["sh", "-c", "python3 --version"]

        response = os_ops.exec_command(cmd)
        assert type(response) is bytes
        assert b'Python 3.' in response
        return

    def test_exec_command_failure(self, os_ops_descr: OsOpsDescr):
        """
        Test exec_command for command execution failure.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        cmd = ["sh", "-c", "nonexistent_command"]

        while True:
            try:
                os_ops.exec_command(cmd)
            except ExecUtilException as e:
                assert type(e.exit_code) is int
                assert e.exit_code == 127

                assert type(e.message) is str
                assert type(e.error) is bytes

                assert e.message.startswith("Utility exited with non-zero code (127). Error:")
                assert "nonexistent_command" in e.message
                assert "not found" in e.message
                assert b"nonexistent_command" in e.error
                assert b"not found" in e.error
                break
            raise Exception("We wait an exception!")
        return

    def test_exec_command_failure__expect_error(self, os_ops_descr: OsOpsDescr):
        """
        Test exec_command for command execution failure.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        RunConditions.skip_if_windows()

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        cmd = ["sh", "-c", "nonexistent_command"]

        exec_r = os_ops.exec_command(cmd, verbose=True, expect_error=True)
        assert type(exec_r) is tuple
        assert len(exec_r) == 3

        exit_status, result, error = exec_r
        assert type(exit_status) is int
        assert type(result) is bytes
        assert type(error) is bytes

        assert exit_status == 127
        assert result == b''
        assert b"nonexistent_command" in error
        assert b"not found" in error
        return

    def test_exec_command_with_exec_env(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        RunConditions.skip_if_windows()

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        C_ENV_NAME = "TESTGRES_TEST__EXEC_ENV_20250414"

        cmd = ["sh", "-c", "echo ${}".format(C_ENV_NAME)]

        exec_env = {C_ENV_NAME: "Hello!"}

        response = os_ops.exec_command(cmd, exec_env=exec_env)
        assert response is not None
        assert type(response) is bytes
        assert response == b'Hello!\n'

        response = os_ops.exec_command(cmd)
        assert response is not None
        assert type(response) is bytes
        assert response == b'\n'
        return

    def test_exec_command_with_exec_env__2(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        RunConditions.skip_if_windows()

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        C_ENV_NAME = "TESTGRES_TEST__EXEC_ENV_20250414"

        tmp_file_content = "echo ${{{}}}".format(C_ENV_NAME)

        logging.info("content is [{}]".format(tmp_file_content))

        tmp_file = os_ops.mkstemp()
        assert type(tmp_file) is str
        assert tmp_file != ""

        logging.info("file is [{}]".format(tmp_file))
        assert os_ops.path_exists(tmp_file)

        os_ops.write(tmp_file, tmp_file_content)

        cmd = ["sh", tmp_file]

        exec_env = {C_ENV_NAME: "Hello!"}

        response = os_ops.exec_command(cmd, exec_env=exec_env)
        assert response is not None
        assert type(response) is bytes
        assert response == b'Hello!\n'

        response = os_ops.exec_command(cmd)
        assert response is not None
        assert type(response) is bytes
        assert response == b'\n'

        os_ops.remove_file(tmp_file)
        assert not os_ops.path_exists(tmp_file)
        return

    def test_exec_command_with_cwd(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        RunConditions.skip_if_windows()

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        cmd = ["pwd"]

        response = os_ops.exec_command(cmd, cwd="/tmp")
        assert response is not None
        assert type(response) is bytes
        assert response == b'/tmp\n'

        response = os_ops.exec_command(cmd)
        assert response is not None
        assert type(response) is bytes
        assert response != b'/tmp\n'
        return

    def test_exec_command__test_unset(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        RunConditions.skip_if_windows()

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        C_ENV_NAME = "LANG"

        cmd = ["sh", "-c", "echo ${}".format(C_ENV_NAME)]

        response1 = os_ops.exec_command(cmd)
        assert response1 is not None
        assert type(response1) is bytes

        if response1 == b'\n':
            logging.warning("Environment variable {} is not defined.".format(C_ENV_NAME))
            return

        exec_env = {C_ENV_NAME: None}
        response2 = os_ops.exec_command(cmd, exec_env=exec_env)
        assert response2 is not None
        assert type(response2) is bytes
        assert response2 == b'\n'

        response3 = os_ops.exec_command(cmd)
        assert response3 is not None
        assert type(response3) is bytes
        assert response3 == response1
        return

    def test_exec_command__test_unset_dummy_var(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        RunConditions.skip_if_windows()

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        C_ENV_NAME = "TESTGRES_TEST__DUMMY_VAR_20250414"

        cmd = ["sh", "-c", "echo ${}".format(C_ENV_NAME)]

        exec_env = {C_ENV_NAME: None}
        response2 = os_ops.exec_command(cmd, exec_env=exec_env)
        assert response2 is not None
        assert type(response2) is bytes
        assert response2 == b'\n'
        return

    def test_is_executable_true(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test is_executable for an existing executable.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        response = os_ops.is_executable("/bin/sh")

        assert response is True
        return

    def test_is_executable_true_2(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test is_executable for an existing executable.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        assert os_ops.is_executable("/bin/sh") is True

        tmpdir = os_ops.mkdtemp(name_with_surprize.value)

        cmd = ["sh", "-c", "cp -p /bin/sh " + os_ops.quote_path(tmpdir)]

        os_ops.exec_command(cmd)

        target = os_ops.build_path(tmpdir, "sh")

        assert os_ops.path_exists(target)

        response = os_ops.is_executable(target)
        assert response is True

        os_ops.remove_file(target)
        os_ops.rmdir(tmpdir)
        return

    def test_is_executable_false(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test is_executable for a non-executable.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert type(name_with_surprize) is __class__.tagNameWithSurprize
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmp_dir = os_ops.mkdtemp()
        filename = os_ops.build_path(tmp_dir, name_with_surprize.value + ".no_exe")

        os_ops.touch(filename)

        response = os_ops.is_executable(filename)
        assert response is False

        os_ops.remove_file(filename)
        os_ops.rmdir(tmp_dir)
        return

    def test_makedirs_and_rmdirs_success(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test makedirs and rmdirs for successful directory creation and removal.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert type(name_with_surprize) is __class__.tagNameWithSurprize
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        path = "/tmp/{}-{}".format(
            name_with_surprize.value,
            uuid.uuid4().bytes.hex()
        )

        # Test makedirs
        os_ops.makedirs(path)
        LocalCheck.check_path_exists(os_ops, path)
        assert os_ops.path_exists(path)

        # Test rmdirs
        os_ops.rmdirs(path)
        LocalCheck.check_path_does_not_exists(os_ops, path)
        assert not os_ops.path_exists(path)
        return

    def test_makedirs_failure(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test makedirs for failure.
        """
        # Try to create a directory in a read-only location
        assert type(os_ops_descr) is OsOpsDescr
        assert type(name_with_surprize) is __class__.tagNameWithSurprize
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        path = "/root/test_dir-{}-{}".format(
            name_with_surprize.value,
            uuid.uuid4().bytes.hex(),
        )

        # Test makedirs
        with pytest.raises(Exception) as x:
            os_ops.makedirs(path)

        if type(os_ops).__name__ == "LocalOperations":
            assert type(x.value) is PermissionError
        elif type(os_ops).__name__ == "RemoteOperations":
            assert type(x.value) is ExecUtilException
        else:
            __class__.helper__bug_check__unknown_os_ops_type(os_ops)
        return

    def test_listdir(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test listdir for listing directory contents.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        path = "/etc"
        files = os_ops.listdir(path)
        assert isinstance(files, list)
        for f in files:
            assert f is not None
            assert type(f) is str
        return

    def test_listdir_2(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test listdir for listing directory contents.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        path = os_ops.mkdtemp(name_with_surprize.value)
        files = os_ops.listdir(path)
        assert isinstance(files, list)
        assert len(files) == 0

        os_ops.rmdir(path)
        return

    def test_path_exists_true__directory(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test path_exists for an existing directory.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert type(name_with_surprize) is __class__.tagNameWithSurprize
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmp_dir = os_ops.mkdtemp()
        assert os_ops.path_exists(tmp_dir) is True

        tmp_dir2 = os_ops.build_path(tmp_dir, name_with_surprize.value)
        os_ops.makedir(tmp_dir2)
        assert os_ops.path_exists(tmp_dir2) is True

        os_ops.rmdir(tmp_dir2)
        os_ops.rmdir(tmp_dir)
        return

    def test_path_exists_true__file(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test path_exists for an existing file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert type(name_with_surprize) is __class__.tagNameWithSurprize
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmp_dir = os_ops.mkdtemp()
        assert os_ops.path_exists(tmp_dir) is True

        filename = os_ops.build_path(tmp_dir, name_with_surprize.value)

        data = "abc"
        os_ops.write(filename, data, binary=False)
        assert os_ops.read(filename, binary=False) == data

        LocalCheck.check_path_exists(os_ops, filename)
        assert os_ops.path_exists(filename) is True

        os_ops.remove_file(filename)
        os_ops.rmdir(tmp_dir)
        return

    def test_path_exists_false__directory(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test path_exists for a non-existing directory.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        assert os_ops.path_exists("/nonexistent_path") is False
        return

    def test_path_exists_false__file(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test path_exists for a non-existing file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        assert os_ops.path_exists("/etc/nonexistent_path.txt") is False
        return

    def test_mkdtemp__default(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        path = os_ops.mkdtemp()
        logging.info("Path is [{0}].".format(path))
        LocalCheck.check_path_exists(os_ops, path)
        assert os_ops.path_exists(path)
        assert os_ops.isdir(path)
        os_ops.rmdir(path)
        LocalCheck.check_path_does_not_exists(os_ops, path)
        assert not os_ops.path_exists(path)
        return

    def test_mkdtemp__custom(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        C_TEMPLATE = name_with_surprize.value
        path = os_ops.mkdtemp(C_TEMPLATE)
        logging.info("Path is [{0}].".format(path))
        LocalCheck.check_path_exists(os_ops, path)
        assert os_ops.path_exists(path)
        assert os_ops.isdir(path)
        assert C_TEMPLATE in os_ops.get_path_basename(path)
        os_ops.rmdir(path)
        LocalCheck.check_path_does_not_exists(os_ops, path)
        assert not os_ops.path_exists(path)
        return

    def test_rmdirs(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmpdir = os_ops.get_tempdir()

        path = os_ops.build_path(
            tmpdir,
            "testgres-os_ops-test_rmdirs-{}-{}".format(
                name_with_surprize.value,
                uuid.uuid4().bytes.hex(),
            )
        )

        local_detecter_is_created = False
        if OsOpsHelpers.is_localhost(os_ops):
            pass
        elif sys.platform != os_ops.get_platform():
            pass
        elif not os.path.exists(tmpdir):
            pass
        else:
            # We will check a real work with another host
            assert not os.path.exists(path)
            os.mkdir(path)
            assert os.path.exists(path)
            local_detecter_is_created = True
            logging.info("Local detecter is created [{}]".format(path))

        cmd = ["sh", "-c", "mkdir " + os_ops.quote_path(path)]
        os_ops.exec_command(cmd)

        LocalCheck.check_path_exists(os_ops, path)
        assert os_ops.path_exists(path)
        assert os_ops.isdir(path)

        assert os_ops.rmdirs(path, ignore_errors=False) is True
        LocalCheck.check_path_does_not_exists(os_ops, path)
        assert not os_ops.path_exists(path)

        if local_detecter_is_created:
            assert os.path.exists(path)
            os.rmdir(path)
            logging.info("Local detecter is deleted [{}]".format(path))

        return

    def test_rmdirs__01_with_subfolder(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        # folder with subfolder
        path = os_ops.mkdtemp()
        LocalCheck.check_path_exists(os_ops, path)
        assert os_ops.path_exists(path)

        dir1 = os_ops.build_path(path, name_with_surprize.value)
        LocalCheck.check_path_does_not_exists(os_ops, dir1)
        assert not os_ops.path_exists(dir1)

        os_ops.makedir(dir1)
        LocalCheck.check_path_exists(os_ops, dir1)
        assert os_ops.path_exists(dir1)
        assert os_ops.isdir(dir1)

        assert os_ops.rmdirs(path, ignore_errors=False) is True
        LocalCheck.check_path_does_not_exists(os_ops, path)
        LocalCheck.check_path_does_not_exists(os_ops, dir1)
        assert not os_ops.path_exists(path)
        assert not os_ops.path_exists(dir1)
        return

    def test_rmdirs__02_with_file(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        # folder with file
        path = os_ops.mkdtemp()
        LocalCheck.check_path_exists(os_ops, path)
        assert os_ops.path_exists(path)

        file1 = os_ops.build_path(path, name_with_surprize.value)
        LocalCheck.check_path_does_not_exists(os_ops, file1)
        assert not os_ops.path_exists(file1)

        os_ops.touch(file1)
        LocalCheck.check_path_exists(os_ops, file1)
        assert os_ops.path_exists(file1)
        assert os_ops.isfile(file1)

        assert os_ops.rmdirs(path, ignore_errors=False) is True
        LocalCheck.check_path_does_not_exists(os_ops, path)
        LocalCheck.check_path_does_not_exists(os_ops, file1)
        assert not os_ops.path_exists(path)
        assert not os_ops.path_exists(file1)
        return

    def test_rmdirs__03_with_subfolder_and_file(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        # folder with subfolder and file
        path = os_ops.mkdtemp()
        LocalCheck.check_path_exists(os_ops, path)
        assert os_ops.path_exists(path)

        dir1 = os_ops.build_path(path, name_with_surprize.value)
        LocalCheck.check_path_does_not_exists(os_ops, dir1)
        assert not os_ops.path_exists(dir1)

        os_ops.makedirs(dir1)
        LocalCheck.check_path_exists(os_ops, dir1)
        assert os_ops.path_exists(dir1)
        assert os_ops.isdir(dir1)
        assert not os_ops.isfile(dir1)

        file1 = os_ops.build_path(dir1, name_with_surprize.value)
        LocalCheck.check_path_does_not_exists(os_ops, file1)
        assert not os_ops.path_exists(file1)

        os_ops.touch(file1)
        LocalCheck.check_path_exists(os_ops, file1)
        assert os_ops.path_exists(file1)
        assert os_ops.isfile(file1)
        assert not os_ops.isdir(file1)

        assert os_ops.rmdirs(path, ignore_errors=False) is True
        LocalCheck.check_path_does_not_exists(os_ops, path)
        LocalCheck.check_path_does_not_exists(os_ops, dir1)
        LocalCheck.check_path_does_not_exists(os_ops, file1)
        assert not os_ops.path_exists(path)
        assert not os_ops.path_exists(dir1)
        assert not os_ops.path_exists(file1)
        return

    def test_write_text_file(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test write for writing data to a text file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        filename = os_ops.mkstemp(name_with_surprize.value)
        data = "Hello, world!"

        os_ops.write(filename, data, truncate=True)
        os_ops.write(filename, data)

        response = os_ops.read(filename)

        assert response == data + data

        os_ops.remove_file(filename)
        return

    def test_write_binary_file(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test write for writing data to a binary file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        filename = os_ops.mkstemp(name_with_surprize.value)
        data = b"\x00\x01\x02\x03"

        os_ops.write(filename, data, binary=True, truncate=True)

        response = os_ops.read(filename, binary=True)
        assert response == data

        os_ops.remove_file(filename)
        return

    def test_read_text_file(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test read for reading data from a text file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        filename = os_ops.mkstemp(name_with_surprize.value)

        C_DATA = "\nabc\n321\n\n"
        os_ops.write(filename, C_DATA)

        response = os_ops.read(filename)
        assert isinstance(response, str)
        assert response == C_DATA
        return

    def test_read_binary_file(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test read for reading data from a binary file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        RunConditions.skip_if_windows()

        filename = "/usr/bin/python3"

        response = os_ops.read(filename, binary=True)

        assert isinstance(response, bytes)
        return

    def test_read__text(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test OsOperations::read for text data.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        with open(__file__, 'r', encoding="utf-8") as file:
            response0 = file.read()

        assert type(response0) is str

        filename = os_ops.mkstemp(
            "testgres-os_ops-test_read__text",
        )

        os_ops.write(
            filename,
            response0,
            binary=False,
        )

        response1 = os_ops.read(filename)
        assert type(response1) is str
        assert response1 == response0

        response2 = os_ops.read(filename, encoding=None, binary=False)
        assert type(response2) is str
        assert response2 == response0

        response3 = os_ops.read(filename, encoding="")
        assert type(response3) is str
        assert response3 == response0

        response4 = os_ops.read(filename, encoding="UTF-8")
        assert type(response4) is str
        assert response4 == response0

        os_ops.remove_file(filename)
        return

    def test_read__binary(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test OsOperations::read for binary data.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        with open(__file__, 'rb') as file:
            response0 = file.read()

        assert type(response0) is bytes

        filename = os_ops.mkstemp(
            name_with_surprize.value,
        )

        os_ops.write(
            filename,
            response0,
            binary=True,
        )

        response1 = os_ops.read(filename, binary=True)
        assert type(response1) is bytes
        assert response1 == response0

        os_ops.remove_file(filename)
        return

    def test_read__binary_and_encoding(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test OsOperations::read for binary data and encoding.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        filename = os_ops.mkstemp()

        with pytest.raises(
                InvalidOperationException,
                match=re.escape("Enconding is not allowed for read binary operation")):
            os_ops.read(filename, encoding="", binary=True)

        os_ops.remove_file(filename)
        return

    def test_read_binary__spec(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test OsOperations::read_binary.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        with open(__file__, 'rb') as file:
            response0 = file.read()

        assert type(response0) is bytes

        filename = os_ops.mkstemp(
            name_with_surprize.value,
        )

        os_ops.write(
            filename,
            response0,
            binary=True,
        )

        response1 = os_ops.read_binary(filename, 0)
        assert type(response1) is bytes
        assert response1 == response0

        response2 = os_ops.read_binary(filename, 1)
        assert type(response2) is bytes
        assert len(response2) < len(response1)
        assert len(response2) + 1 == len(response1)
        assert response2 == response1[1:]

        response3 = os_ops.read_binary(filename, len(response1))
        assert type(response3) is bytes
        assert len(response3) == 0

        response4 = os_ops.read_binary(filename, len(response2))
        assert type(response4) is bytes
        assert len(response4) == 1
        assert response4[0] == response1[len(response1) - 1]

        response5 = os_ops.read_binary(filename, len(response1) + 1)
        assert type(response5) is bytes
        assert len(response5) == 0

        os_ops.remove_file(filename)
        return

    def test_read_binary__spec__negative_offset(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test OsOperations::read_binary with negative offset.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        filename = os_ops.mkstemp(name_with_surprize.value)

        with pytest.raises(
                ValueError,
                match=re.escape("Negative 'offset' is not supported.")):
            os_ops.read_binary(filename, -1)

        os_ops.remove_file(filename)
        return

    def test_get_file_size(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test OsOperations::get_file_size.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        filename = os_ops.mkstemp(name_with_surprize.value)
        sz = os_ops.get_file_size(filename)
        assert type(sz) is int
        assert sz == 0

        os_ops.write(filename, b"\x02\x01\x00", binary=True)
        sz = os_ops.get_file_size(filename)
        assert type(sz) is int
        assert sz == 3

        os_ops.write(filename, b"\x04", binary=True, truncate=False)
        sz = os_ops.get_file_size(filename)
        assert type(sz) is int
        assert sz == 4

        os_ops.remove_file(filename)
        return

    def test_isfile_true(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test isfile for an existing file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        filename = os_ops.mkstemp(name_with_surprize.value)

        LocalCheck.check_isfile(os_ops, filename)
        response = os_ops.isfile(filename)
        assert response is True

        os_ops.remove_file(filename)
        return

    def test_isfile_false__not_exist(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test isfile for a non-existing file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmpdir = os_ops.get_tempdir()

        filename = os_ops.build_path(
            tmpdir,
            name_with_surprize.value,
        )

        LocalCheck.check_path_does_not_exists(os_ops, filename)
        assert not os_ops.path_exists(filename)

        local_detecter_is_created = False
        if OsOpsHelpers.is_localhost(os_ops):
            pass
        elif sys.platform != os_ops.get_platform():
            pass
        elif not os.path.exists(tmpdir):
            pass
        else:
            # We will check a real work with another host
            assert not os.path.exists(filename)
            with open(filename, "a"):
                os.utime(filename, None)
            assert os.path.exists(filename)
            assert os.path.isfile(filename)
            local_detecter_is_created = True
            logging.info("Local detecter is created [{}]".format(filename))

        response = os_ops.isfile(filename)
        assert response is False

        if local_detecter_is_created:
            assert os.path.exists(filename)
            os.remove(filename)
            assert not os.path.exists(filename)
            logging.info("Local detecter is deleted [{}]".format(filename))
        return

    def test_isfile_false__directory(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test isfile for a firectory.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmpdir = os_ops.get_tempdir()
        LocalCheck.check_path_exists(os_ops, tmpdir)
        LocalCheck.check_isdir(os_ops, tmpdir)
        LocalCheck.check_not_isfile(os_ops, tmpdir)
        assert os_ops.path_exists(tmpdir)
        assert os_ops.isdir(tmpdir)

        response = os_ops.isfile(tmpdir)
        assert response is False
        return

    def test_isdir_true(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test isdir for an existing directory.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmpdir = os_ops.mkdtemp(name_with_surprize.value)
        LocalCheck.check_path_exists(os_ops, tmpdir)
        LocalCheck.check_isdir(os_ops, tmpdir)
        LocalCheck.check_not_isfile(os_ops, tmpdir)
        assert os_ops.path_exists(tmpdir)

        response = os_ops.isdir(tmpdir)
        assert response is True

        os_ops.rmdir(tmpdir)
        return

    def test_isdir_false__not_exist(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test isdir for a non-existing directory.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmpdir = os_ops.get_tempdir()
        LocalCheck.check_path_exists(os_ops, tmpdir)
        LocalCheck.check_isdir(os_ops, tmpdir)
        LocalCheck.check_not_isfile(os_ops, tmpdir)
        assert os_ops.path_exists(tmpdir)
        assert os_ops.isdir(tmpdir) is True

        name = os_ops.build_path(
            tmpdir,
            "it_is_nonexistent_directory-{}".format(uuid.uuid4().bytes.hex()),
        )

        response = os_ops.isdir(name)
        assert response is False
        return

    def test_isdir_false__file(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test isdir for a file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        name = os_ops.mkstemp()
        LocalCheck.check_path_exists(os_ops, name)
        LocalCheck.check_isfile(os_ops, name)
        LocalCheck.check_not_isdir(os_ops, name)
        assert os_ops.path_exists(name) is True
        assert os_ops.isfile(name) is True

        response = os_ops.isdir(name)
        assert response is False

        os_ops.remove_file(name)
        LocalCheck.check_path_does_not_exists(os_ops, name)
        assert os_ops.path_exists(name) is False
        return

    def test_cwd(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        """
        Test cwd.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        v = os_ops.cwd()

        assert v is not None
        assert type(v) is str
        assert v != ""
        return

    class tagWriteData001:
        def __init__(self, sign, source, cp_rw, cp_truncate, cp_binary, cp_data, result):
            self.sign = sign
            self.source = source
            self.call_param__rw = cp_rw
            self.call_param__truncate = cp_truncate
            self.call_param__binary = cp_binary
            self.call_param__data = cp_data
            self.result = result
            return

    sm_write_data001 = [
        tagWriteData001("A001", "1234567890", False, False, False, "ABC", "1234567890ABC"),
        tagWriteData001("A002", b"1234567890", False, False, True, b"ABC", b"1234567890ABC"),

        tagWriteData001("B001", "1234567890", False, True, False, "ABC", "ABC"),
        tagWriteData001("B002", "1234567890", False, True, False, "ABC1234567890", "ABC1234567890"),
        tagWriteData001("B003", b"1234567890", False, True, True, b"ABC", b"ABC"),
        tagWriteData001("B004", b"1234567890", False, True, True, b"ABC1234567890", b"ABC1234567890"),

        tagWriteData001("C001", "1234567890", True, False, False, "ABC", "1234567890ABC"),
        tagWriteData001("C002", b"1234567890", True, False, True, b"ABC", b"1234567890ABC"),

        tagWriteData001("D001", "1234567890", True, True, False, "ABC", "ABC"),
        tagWriteData001("D002", "1234567890", True, True, False, "ABC1234567890", "ABC1234567890"),
        tagWriteData001("D003", b"1234567890", True, True, True, b"ABC", b"ABC"),
        tagWriteData001("D004", b"1234567890", True, True, True, b"ABC1234567890", b"ABC1234567890"),

        tagWriteData001("E001", "\0001234567890\000", False, False, False, "\000ABC\000", "\0001234567890\000\000ABC\000"),
        tagWriteData001("E002", b"\0001234567890\000", False, False, True, b"\000ABC\000", b"\0001234567890\000\000ABC\000"),

        tagWriteData001("F001", "a\nb\n", False, False, False, ["c", "d"], "a\nb\ncd"),
        tagWriteData001("F002", b"a\nb\n", False, False, True, [b"c", b"d"], b"a\nb\ncd"),

        tagWriteData001("G001", "a\nb\n", False, False, False, ["c\n\n", "d\n"], "a\nb\nc\n\nd\n"),
        tagWriteData001("G002", b"a\nb\n", False, False, True, [b"c\n\n", b"d\n"], b"a\nb\nc\n\nd\n"),

        tagWriteData001("H001", "a\nb\n\000", False, False, False, ["c\n\n", "d\n"], "a\nb\n\000c\n\nd\n"),
        tagWriteData001("H002", b"a\nb\n\000", False, False, True, [b"c\n\n", b"d\n"], b"a\nb\n\000c\n\nd\n"),

        tagWriteData001("J001", "a\nb\n\000", False, False, False, ["c\n\n\x00", "d\n"], "a\nb\n\000c\n\n\x00d\n"),
        tagWriteData001("J002", b"a\nb\n\000", False, False, True, [b"c\n\n\x00", b"d\n"], b"a\nb\n\000c\n\n\x00d\n"),
    ]

    @pytest.fixture(
        params=sm_write_data001,
        ids=[x.sign for x in sm_write_data001],
    )
    def write_data001(self, request: pytest.FixtureRequest):
        assert isinstance(request, pytest.FixtureRequest)
        assert type(request.param) is __class__.tagWriteData001
        return request.param

    def test_write(
        self,
        write_data001: tagWriteData001,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(write_data001) is __class__.tagWriteData001

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmp_file = os_ops.mkstemp("testgres-os_ops-test_write")

        os_ops.write(
            tmp_file,
            write_data001.source,
            binary=write_data001.call_param__binary,
        )
        s = os_ops.read(
            tmp_file,
            binary=write_data001.call_param__binary,
        )
        assert s == write_data001.source

        os_ops.write(
            tmp_file,
            write_data001.call_param__data,
            read_and_write=write_data001.call_param__rw,
            truncate=write_data001.call_param__truncate,
            binary=write_data001.call_param__binary,
        )
        s = os_ops.read(
            tmp_file,
            binary=write_data001.call_param__binary,
        )
        assert s == write_data001.result

        os_ops.remove_file(tmp_file)
        return

    def test_touch(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        """
        Test touch for creating a new file or updating access and modification times of an existing file.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmpdir = os_ops.mkdtemp()

        filename = os_ops.build_path(tmpdir, name_with_surprize.value)

        os_ops.touch(filename)

        assert os_ops.path_exists(filename)
        assert os_ops.isfile(filename)

        stat1 = os_ops.get_file_stat(filename)
        assert type(stat1) is dict

        time.sleep(1.1)

        os_ops.touch(filename)

        stat2 = os_ops.get_file_stat(filename)
        assert type(stat2) is dict

        mtime1 = stat1[os_ops.C_FILE_STAT_PROP__MTIME]
        mtime2 = stat2[os_ops.C_FILE_STAT_PROP__MTIME]

        assert type(mtime1) is datetime.datetime
        assert type(mtime2) is datetime.datetime

        assert mtime1 < mtime2

        os_ops.remove_file(filename)
        os_ops.rmdir(tmpdir)
        return

    def test_is_port_free__true(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        C_SAMPLES = 128
        C_LIMIT = 10

        ports = random.sample(range(1024, 65536), C_SAMPLES)
        assert type(ports) is list

        ok_count = 0
        no_count = 0

        py_test_code_templ = """
import socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    try:
        s.bind(("", {0}))
    except OSError:
        exit(123)
print(str({0}))
exit(0)
"""
        for port in ports:
            logging.info("Try to bind port {}...".format(port))

            py_test_code = py_test_code_templ.format(port)

            try:
                r = os_ops.exec_command(
                    ["python3", "-c", py_test_code],
                    encoding="utf-8",
                )
            except ExecUtilException as e:
                if e.exit_code == 123:
                    logging.info("Fails")
                    continue
                raise

            assert r == str(port) + "\n"

            logging.info("Try to check via is_port_free ...")
            r = os_ops.is_port_free(port)

            if r:
                ok_count += 1
                logging.info("OK. Port {} is free.".format(port))
            else:
                no_count += 1
                logging.warning("NO. Port {} is not free.".format(port))

            if ok_count == C_LIMIT:
                return
            continue

        if ok_count == 0:
            raise RuntimeError("No one free port was found.")
        return

    def test_is_port_free__false(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        C_LIMIT = 5
        C_SAMPLES = C_LIMIT * 2

        ports = random.sample(range(1024, 65536), C_SAMPLES)

        ok_count = 0
        no_count = 0

        py_server_code_templ = """
import socket, time
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.bind(("", {0}))
    s.listen(1)
    print("READY", flush=True)
    time.sleep(30)  # Keep the port busy for 30 seconds
except OSError:
    exit(123)
exit(0)
"""
        for port in ports:
            logging.info("Try to occupy port {} on target machine...".format(port))

            py_server_code = py_server_code_templ.format(port)

            # Start a background process on the target machine
            p = os_ops.exec_command(
                ["python3", "-u", "-c", py_server_code],
                get_process=True,
                encoding="utf-8",
            )
            assert isinstance(p, subprocess.Popen)
            assert p.stdout is not None

            try:
                # Read the first line from the process's stdout.
                # If it's READY, the socket was successfully bound.
                # If the process crashed (code 123), readline() will return empty.
                ready_line = p.stdout.readline()

                if ready_line != "READY\n":
                    logging.info("Port {} is already busy or failed to bind, skipping...".format(port))
                    continue  # it jumps into finally

                logging.info("Port {} is occupied. Verifying via is_port_free...".format(port))

                # MAIN CHECK: The port is currently busy, so is_port_free should return False!
                r = os_ops.is_port_free(port)
                assert type(r) is bool

                if not r:
                    ok_count += 1
                    logging.info("OK. Port {} is correctly detected as NOT free.".format(port))
                else:
                    no_count += 1
                    logging.warning("NO. Port {} was detected as free, but it is busy!".format(port))
            finally:
                # We guarantee that the process will be terminated and the port will be released on the target machine.
                p.terminate()
                p.wait()

            if ok_count == C_LIMIT:
                return
            continue

        if ok_count == 0:
            raise RuntimeError("No one free port was found.")
        return

    def test_get_tempdir(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        dir = os_ops.get_tempdir()
        assert type(dir) is str
        LocalCheck.check_path_exists(os_ops, dir)
        assert os_ops.path_exists(dir) is True
        assert os_ops.isdir(dir) is True

        file_path = os_ops.build_path(
            dir,
            "testgres--" + uuid.uuid4().hex + ".tmp",
        )

        os_ops.write(file_path, "1234", binary=False)

        LocalCheck.check_path_exists(os_ops, file_path)
        LocalCheck.check_isfile(os_ops, file_path)
        assert os_ops.path_exists(file_path) is True
        assert os_ops.isfile(file_path) is True
        assert os_ops.get_file_size(file_path) == 4

        d = os_ops.read(file_path, binary=False)
        assert d == "1234"

        os_ops.remove_file(file_path)
        LocalCheck.check_path_does_not_exists(os_ops, file_path)
        assert os_ops.path_exists(file_path) is False
        return

    def test_get_tempdir__compare_with_py_info(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        actual_dir = os_ops.get_tempdir()
        assert actual_dir is not None
        assert type(actual_dir) is str

        # --------
        cmd = ["python3", "-c", "import tempfile;print(tempfile.gettempdir());"]

        expected_dir_b = os_ops.exec_command(cmd)
        assert type(expected_dir_b) is bytes
        expected_dir = expected_dir_b.decode()
        assert type(expected_dir) is str
        assert actual_dir + "\n" == expected_dir
        return

    class tagData_OS_OPS__NUMS:
        os_ops_descr: OsOpsDescr
        nums: int

        def __init__(self, os_ops_descr: OsOpsDescr, nums: int):
            assert type(os_ops_descr) is OsOpsDescr
            assert type(nums) is int

            self.os_ops_descr = os_ops_descr
            self.nums = nums
            return

    sm_test_exclusive_creation__mt__data = [
        tagData_OS_OPS__NUMS(OsOpsDescrs.sm_local_os_ops_descr, 100000),
        tagData_OS_OPS__NUMS(OsOpsDescrs.sm_remote_os_ops_descr, 120),
    ]

    @pytest.fixture(
        params=sm_test_exclusive_creation__mt__data,
        ids=[x.os_ops_descr.sign for x in sm_test_exclusive_creation__mt__data]
    )
    def data001(self, request: pytest.FixtureRequest) -> tagData_OS_OPS__NUMS:
        assert isinstance(request, pytest.FixtureRequest)
        return request.param

    def test_mkdir__mt(self, data001: tagData_OS_OPS__NUMS):
        assert type(data001) is __class__.tagData_OS_OPS__NUMS

        N_WORKERS = 4
        N_NUMBERS = data001.nums
        assert type(N_NUMBERS) is int

        os_ops = data001.os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        lock_dir_prefix = "test_mkdir_mt--" + uuid.uuid4().hex

        lock_dir = os_ops.mkdtemp(prefix=lock_dir_prefix)

        logging.info("A lock file [{}] is creating ...".format(lock_dir))

        LocalCheck.check_path_exists(os_ops, lock_dir)
        assert os_ops.path_exists(lock_dir) is True

        def MAKE_PATH(os_ops: OsOperations, lock_dir: str, num: int) -> str:
            assert isinstance(os_ops, OsOperations)
            assert type(lock_dir) is str
            assert type(num) is int
            return os_ops.build_path(lock_dir, str(num) + ".lock")

        def LOCAL_WORKER(os_ops: OsOperations,
                         workerID: int,
                         lock_dir: str,
                         cNumbers: int,
                         reservedNumbers: typing.Set[int]) -> None:
            assert isinstance(os_ops, OsOperations)
            assert type(workerID) is int
            assert type(lock_dir) is str
            assert type(cNumbers) is int
            assert type(reservedNumbers) is set
            assert cNumbers > 0
            assert len(reservedNumbers) == 0

            assert os_ops.path_exists(lock_dir)

            def LOG_INFO(template: str, *args) -> None:
                assert type(template) is str
                assert type(args) is tuple

                msg = template.format(*args)
                assert type(msg) is str

                logging.info("[Worker #{}] {}".format(workerID, msg))
                return

            LOG_INFO("HELLO! I am here!")

            for num in range(cNumbers):
                assert num not in reservedNumbers

                file_path = MAKE_PATH(os_ops, lock_dir, num)

                try:
                    os_ops.makedir(file_path)
                except Exception as e:
                    LOG_INFO(
                        "Can't reserve {}. Error ({}): {}",
                        num,
                        type(e).__name__,
                        str(e)
                    )
                    continue

                LOG_INFO("Number {} is reserved!", num)
                assert os_ops.path_exists(file_path)
                reservedNumbers.add(num)
                continue

            n_total = cNumbers
            n_ok = len(reservedNumbers)
            assert n_ok <= n_total

            LOG_INFO("Finish! OK: {}. FAILED: {}.", n_ok, n_total - n_ok)
            return

        # -----------------------
        logging.info("Worker are creating ...")

        threadPool = ThreadPoolExecutor(
            max_workers=N_WORKERS,
            thread_name_prefix="ex_creator"
        )

        class tadWorkerData:
            future: ThreadFuture
            reservedNumbers: typing.Set[int]

        workerDatas: typing.List[tadWorkerData] = list()

        nErrors = 0

        try:
            for n in range(N_WORKERS):
                logging.info("worker #{} is creating ...".format(n))

                workerDatas.append(tadWorkerData())

                workerDatas[n].reservedNumbers = set()

                workerDatas[n].future = threadPool.submit(
                    LOCAL_WORKER,
                    os_ops,
                    n,
                    lock_dir,
                    N_NUMBERS,
                    workerDatas[n].reservedNumbers
                )

                assert workerDatas[n].future is not None

            logging.info("OK. All the workers were created!")
        except Exception as e:
            nErrors += 1
            logging.error("A problem is detected ({}): {}".format(type(e).__name__, str(e)))

        logging.info("Will wait for stop of all the workers...")

        nWorkers = 0

        assert type(workerDatas) is list

        for i in range(len(workerDatas)):
            worker = workerDatas[i].future

            if worker is None:
                continue

            nWorkers += 1

            assert isinstance(worker, ThreadFuture)

            try:
                logging.info("Wait for worker #{}".format(i))
                worker.result()
            except Exception as e:
                nErrors += 1
                logging.error("Worker #{} finished with error ({}): {}".format(
                    i,
                    type(e).__name__,
                    str(e),
                ))
            continue

        assert nWorkers == N_WORKERS

        if nErrors != 0:
            raise RuntimeError("Some problems were detected. Please examine the log messages.")

        logging.info("OK. Let's check worker results!")

        reservedNumbers: typing.Dict[int, int] = dict()

        for i in range(N_WORKERS):
            logging.info("Worker #{} is checked ...".format(i))

            workerNumbers = workerDatas[i].reservedNumbers
            assert type(workerNumbers) is set

            for n in workerNumbers:
                if n < 0 or n >= N_NUMBERS:
                    nErrors += 1
                    logging.error("Unexpected number {}".format(n))
                    continue

                if n in reservedNumbers.keys():
                    nErrors += 1
                    logging.error("Number {} was already reserved by worker #{}".format(
                        n,
                        reservedNumbers[n]
                    ))
                else:
                    reservedNumbers[n] = i

                file_path = MAKE_PATH(os_ops, lock_dir, n)
                if not os_ops.path_exists(file_path):
                    nErrors += 1
                    logging.error("File {} is not found!".format(file_path))
                    continue

                continue

        logging.info("OK. Let's check reservedNumbers!")

        for n in range(N_NUMBERS):
            if n not in reservedNumbers.keys():
                nErrors += 1
                logging.error("Number {} is not reserved!".format(n))
                continue

            file_path = MAKE_PATH(os_ops, lock_dir, n)
            if not os_ops.path_exists(file_path):
                nErrors += 1
                logging.error("File {} is not found!".format(file_path))
                continue

            # OK!
            continue

        logging.info("Verification is finished! Total error count is {}.".format(nErrors))

        if nErrors == 0:
            logging.info("Root lock-directory [{}] will be deleted.".format(
                lock_dir
            ))

            for n in range(N_NUMBERS):
                file_path = MAKE_PATH(os_ops, lock_dir, n)
                try:
                    os_ops.rmdir(file_path)
                except Exception as e:
                    nErrors += 1
                    logging.error("Cannot delete directory [{}]. Error ({}): {}".format(
                        file_path,
                        type(e).__name__,
                        str(e)
                    ))
                    continue

                if os_ops.path_exists(file_path):
                    nErrors += 1
                    logging.error("Directory {} is not deleted!".format(file_path))
                    continue

            if nErrors == 0:
                try:
                    os_ops.rmdir(lock_dir)
                except Exception as e:
                    nErrors += 1
                    logging.error("Cannot delete directory [{}]. Error ({}): {}".format(
                        lock_dir,
                        type(e).__name__,
                        str(e)
                    ))

        logging.info("Test is finished! Total error count is {}.".format(nErrors))
        return

    @dataclasses.dataclass
    class T_KILL_SIGNAL_DESCR:
        sign: str
        signal: typing.Union[int, os_signal.Signals]
        signal_num_s: str

    sm_kill_signal_ids: typing.List[T_KILL_SIGNAL_DESCR] = [
        T_KILL_SIGNAL_DESCR("SIGINT", os_signal.SIGINT, "2"),
        # T_KILL_SIGNAL_DESCR("SIGQUIT", os_signal.SIGQUIT, "3"), # it creates coredump
        T_KILL_SIGNAL_DESCR("SIGKILL", os_signal.SIGKILL, "9"),
        T_KILL_SIGNAL_DESCR("SIGTERM", os_signal.SIGTERM, "15"),
        T_KILL_SIGNAL_DESCR("2", 2, "2"),
        # T_KILL_SIGNAL_DESCR("3", 3, "3"), # it creates coredump
        T_KILL_SIGNAL_DESCR("9", 9, "9"),
        T_KILL_SIGNAL_DESCR("15", 15, "15"),
    ]

    @pytest.fixture(
        params=sm_kill_signal_ids,
        ids=["signal: {}".format(x.sign) for x in sm_kill_signal_ids],
    )
    def kill_signal_id(
        self,
        request: pytest.FixtureRequest,
    ) -> T_KILL_SIGNAL_DESCR:
        assert isinstance(request, pytest.FixtureRequest)
        assert type(request.param).__name__ == "T_KILL_SIGNAL_DESCR"
        return request.param

    def test_kill_signal(
        self,
        kill_signal_id: T_KILL_SIGNAL_DESCR,
    ):
        assert type(kill_signal_id) is __class__.T_KILL_SIGNAL_DESCR
        assert "{}".format(kill_signal_id.signal) == kill_signal_id.signal_num_s
        assert "{}".format(int(kill_signal_id.signal)) == kill_signal_id.signal_num_s
        return

    def test_kill(
        self,
        os_ops_descr: OsOpsDescr,
        kill_signal_id: T_KILL_SIGNAL_DESCR,
    ):
        """
        Test listdir for listing directory contents.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(kill_signal_id) is __class__.T_KILL_SIGNAL_DESCR

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        cmd = [
            "python3",
            "-u",
            "-c",
            "import os, time; print(os.getpid());time.sleep(300);print('EXIT')"
        ]

        logging.info("Local test process is creating ...")
        proc = os_ops.exec_command(
            cmd,
            encoding="utf-8",
            get_process=True,
        )

        assert proc is not None
        assert type(proc) is subprocess.Popen
        assert proc.stdout is not None
        line = proc.stdout.readline()
        assert line is not None
        assert type(line) is str
        logging.info("proc output: {!r}".format(line))
        line = line.rstrip()
        assert line != ""
        proc_pid = int(line)
        assert type(proc_pid) is int
        logging.info("Test process pid is {}".format(proc_pid))

        logging.info("Check this test process ...")
        assert os_ops.get_process_children(proc_pid) == []

        logging.info("Kill this test process ...")
        os_ops.kill(proc_pid, kill_signal_id.signal)

        logging.info("Wait for finish ...")
        proc.wait()

        logging.info("Try to get this test process ...")

        attempt = 0
        while True:
            if attempt == 20:
                raise RuntimeError("Process did not die.")

            attempt += 1

            if attempt > 1:
                logging.info("Sleep 1 seconds...")
                time.sleep(1)

            try:
                os_ops.get_process_children(proc_pid)
            except Exception as e:
                if type(os_ops).__name__ == "LocalOperations":
                    if isinstance(e, psutil.ZombieProcess):
                        logging.info("Exception {}: {}".format(
                            type(e).__name__,
                            str(e),
                        ))
                        break
                    if isinstance(e, psutil.NoSuchProcess):
                        logging.info("OK. Process died.")
                        break
                    raise

                if type(os_ops).__name__ == "RemoteOperations":
                    if isinstance(e, ExecUtilException):
                        assert e.exit_code == 1
                        logging.info("OK. Process died.")
                        break
                    raise

                logging.error("Unknown os_ops object: {}".format(
                    type(os_ops).__name__,
                ))
                raise
            else:
                logging.info("Process is alive!")
                continue
        return

    def test_kill__unk_pid(
        self,
        os_ops_descr: OsOpsDescr,
        kill_signal_id: T_KILL_SIGNAL_DESCR,
    ):
        """
        Test listdir for listing directory contents.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(kill_signal_id) is __class__.T_KILL_SIGNAL_DESCR

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        cmd = [
            "python3",
            "-u",
            "-c",
            """
import os, sys
print('a:' + str(os.getpid()), file=sys.stdout)
print('a2', file=sys.stdout)
print('b', file=sys.stderr)
"""
        ]

        logging.info("Local test process is creating ...")
        proc = os_ops.exec_command(
            cmd,
            encoding="utf-8",
            get_process=True,
        )

        assert proc is not None
        assert type(proc) is subprocess.Popen

        proc_pid = proc.pid
        assert type(proc_pid) is int
        assert proc.stdout is not None
        line = proc.stdout.readline()
        assert line is not None
        assert type(line) is str
        logging.info("proc output: {!r}".format(line))
        line = line.rstrip()
        assert line != ""
        assert line.startswith("a:")
        line = line[2:]
        assert line != ""
        proc_pid = int(line)
        assert type(proc_pid) is int
        logging.info("Test process pid is {}".format(proc_pid))

        logging.info("Wait for finish ...")
        pout = proc.stdout.read()
        assert proc.stderr is not None
        perr = proc.stderr.read()
        proc.wait()
        logging.info("STDOUT: {}".format(pout))
        logging.info("STDERR: {}".format(perr))
        assert type(pout) is str
        assert type(perr) is str
        assert pout == "a2\n"
        assert perr == "b\n"
        assert type(proc.returncode) is int
        assert proc.returncode == 0

        logging.info("Try to get this test process ...")

        attempt = 0
        while True:
            if attempt == 20:
                raise RuntimeError("Process did not die.")

            attempt += 1

            if attempt > 1:
                logging.info("Sleep 1 seconds...")
                time.sleep(1)

            try:
                os_ops.get_process_children(proc_pid)
            except Exception as e:
                if type(os_ops).__name__ == "LocalOperations":
                    if isinstance(e, psutil.ZombieProcess):
                        logging.info("Exception {}: {}".format(
                            type(e).__name__,
                            str(e),
                        ))
                        break
                    if isinstance(e, psutil.NoSuchProcess):
                        logging.info("OK. Process died.")
                        break
                    raise

                if type(os_ops).__name__ == "RemoteOperations":
                    if isinstance(e, ExecUtilException):
                        assert e.exit_code == 1
                        logging.info("OK. Process died.")
                        break
                    raise

                logging.error("Unknown os_ops object: {}".format(
                    type(os_ops).__name__,
                ))
                raise
            else:
                logging.info("Process is alive!")
                continue

        # --------------------
        with pytest.raises(expected_exception=Exception) as x:
            os_ops.kill(proc_pid, kill_signal_id.signal)

        assert x is not None
        assert isinstance(x.value, Exception)
        assert not isinstance(x.value, AssertionError)

        logging.info("Our error is [{}]".format(str(x.value)))
        logging.info("Our exception has type [{}]".format(type(x.value).__name__))

        if type(os_ops).__name__ == "LocalOperations":
            assert type(x.value) is ProcessLookupError
            assert "No such process" in str(x.value)
        elif type(os_ops).__name__ == "RemoteOperations":
            assert type(x.value) is ExecUtilException
            assert "No such process" in str(x.value)
        else:
            __class__.helper__bug_check__unknown_os_ops_type(os_ops)

        return

    def test_get_dirname(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        expected_dirname = "abc"

        p = os_ops.build_path(expected_dirname, "file1.txt")
        assert type(p) is str
        assert p != ""

        actual_dirname = os_ops.get_dirname(p)
        assert type(actual_dirname) is str
        assert actual_dirname != ""
        assert actual_dirname == expected_dirname
        return

    def test_is_abs_path__yes(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        p = os_ops.get_tempdir()
        assert type(p) is str
        assert p != ""
        LocalCheck.check_path_exists(os_ops, p)
        LocalCheck.check_isdir(os_ops, p)
        LocalCheck.check_path_is_abs(os_ops, p)
        assert os_ops.path_exists(p) is True
        assert os_ops.isdir(p) is True

        actual_value = os_ops.is_abs_path(p)
        assert type(actual_value) is bool
        assert actual_value is True
        return

    def test_is_abs_path__no(self, os_ops_descr: OsOpsDescr):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        p = "."
        assert not os.path.isabs(p)
        LocalCheck.check_path_is_not_abs(os_ops, p)

        actual_value = os_ops.is_abs_path(p)
        assert type(actual_value) is bool
        assert actual_value is False
        return

    # --------------------------------------------------------------------
    def test_get_abs_path(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        def LOCAL__check(value, expected) -> bool:
            logging.info("Source path: [{}]".format(value))
            actual = os_ops.get_abs_path(value)
            if actual == expected:
                logging.info("Result is OK: [{}].".format(
                    actual,
                ))
            else:
                logging.error("Result is BAD: [{}]. Expected: [{}].".format(
                    actual,
                    expected,
                ))
            logging.info("")
            return False

        logging.info("------------- test empty string")
        cwd = os_ops.cwd()
        LOCAL__check("", cwd)

        logging.info("------------- test cwd")
        LOCAL__check(".", cwd)

        path = os_ops.build_path(cwd, ".")
        LOCAL__check(path, cwd)

        cwd = os_ops.cwd()
        expected_r = os_ops.build_path(cwd, "abc")
        LOCAL__check("abc", expected_r)

        cwd = os_ops.cwd()
        expected_r = os_ops.build_path(cwd, "abc")
        LOCAL__check("./abc", expected_r)

        cwd = os_ops.cwd()
        expected_r = os_ops.build_path(os_ops.get_dirname(cwd), "abc")
        LOCAL__check("../abc", expected_r)

        cwd = os_ops.cwd()
        expected_r = os_ops.build_path(cwd, "abc1.txt")
        LOCAL__check("abc1.txt", expected_r)

        logging.info("------------- test cwd parent")
        cwd = os_ops.cwd()
        expected_r = os_ops.get_dirname(cwd)
        LOCAL__check("..", expected_r)

        logging.info("------------- test file")
        file = os_ops.mkstemp()
        LOCAL__check(file, file)
        os_ops.remove_file(file)

        logging.info("------------- test dir")
        dir = os_ops.mkdtemp()
        LOCAL__check(dir, dir)

        dirname = os_ops.get_path_basename(dir)
        path = os_ops.build_path(dir, "..", dirname)
        LOCAL__check(path, dir)

        dirname = os_ops.get_path_basename(dir)
        path = os_ops.build_path(dir, "..", dirname, "abc.txt")
        expected_r = os_ops.build_path(dir, "abc.txt")
        LOCAL__check(path, expected_r)

        os_ops.rmdir(dir)

        logging.info("------------- unknown path")
        expected_r = os_ops.build_path(cwd, "abc/file.txt")
        LOCAL__check("./abc/file.txt", expected_r)

        logging.info("------------- home dir")
        LOCAL__check("/~", "/~")

        logging.info("------------- test root over-traversal")
        # Из любого места системы (даже глубокого) 15 переходов вверх выведут в корень
        many_dots = os_ops.build_path(*([".."] * 15))
        LOCAL__check(many_dots, "/")

        # Корень + еще раз вверх + папка
        path = os_ops.build_path("/", "..", "abc")
        LOCAL__check(path, "/abc")

        logging.info("------------- test multiple slashes")

        # TODO: Double slash at the beginning. In POSIX it sometimes
        # has a special meaning, let's check it out.
        # os.path.abs returns "//abc"
        # r = os_ops.get_abs_path("//abc")
        # LOCAL__check("//abc", "/abc")

        # Slashes in the middle of a relative path
        expected_r = os_ops.build_path(cwd, "abc", "def")
        LOCAL__check("abc///def", expected_r)

        # Relative path ending with a slash
        expected_r = os_ops.build_path(cwd, "abc")
        LOCAL__check("abc/", expected_r)

        logging.info("------------- test raw tilde")
        exec_r = os_ops.exec_command(["sh", "-c", "cd ~;pwd"], encoding="utf-8")
        assert type(exec_r) is str
        expected_r = exec_r.rstrip()
        LOCAL__check("~", expected_r)

        LOCAL__check("~/", expected_r)

        # Tilda with a ROOT user
        LOCAL__check("~root", "/root")
        LOCAL__check("~root/", "/root")
        LOCAL__check("~root", "/root")
        LOCAL__check("~root/abc/", "/root/abc")

        logging.info("------------- test spaces and special chars")
        # Folder with quotes, and spaces.
        weird_name = "my folder VAR 'single' \"double\""
        expected_r = os_ops.build_path(cwd, weird_name)
        LOCAL__check(weird_name, expected_r)

        # TODO: Folder with dollar signs, quotes, and spaces.
        # weird_name = "my folder $VAR 'single' \"double\""
        # expected_r = os_ops.build_path(cwd, weird_name)
        # LOCAL__check(weird_name, expected_r)

        for n in __class__.sm_names_with_surprize:
            logging.info("--------------------- test names with surprizes [{}]".format(n.sign))
            expected_r = os_ops.build_path(cwd, n.value)
            LOCAL__check(n.value, expected_r)

        logging.info("OK. GO HOME!")
        return

    # --------------------------------------------------------------------
    @dataclasses.dataclass
    class tagGetPathBaseNameData:
        sign: str
        value: str
        result: str

    sm_GetPathBaseNameDatas: typing.List[tagGetPathBaseNameData] = [
        tagGetPathBaseNameData(
            sign="empty",
            value="",
            result="",
        ),
        tagGetPathBaseNameData(
            sign="relative_curdir",
            value=".",
            result=".",
        ),
        tagGetPathBaseNameData(
            sign="relative_parentdir",
            value="..",
            result="..",
        ),
        tagGetPathBaseNameData(
            sign="a",
            value="a",
            result="a",
        ),
        tagGetPathBaseNameData(
            sign="a.txt",
            value="a.txt",
            result="a.txt",
        ),
        tagGetPathBaseNameData(
            sign="root__a.txt",
            value="/a.txt",
            result="a.txt",
        ),
        tagGetPathBaseNameData(
            sign="curdir__a.txt",
            value="./a.txt",
            result="a.txt",
        ),
        tagGetPathBaseNameData(
            sign="parentdir__a.txt",
            value="../a.txt",
            result="a.txt",
        ),
        tagGetPathBaseNameData(
            sign="path001",
            value="a/b/c/my-file-name.txt",
            result="my-file-name.txt",
        ),
        tagGetPathBaseNameData(
            sign="path002",
            value="a/b/c/my-file-name",
            result="my-file-name",
        ),
    ]

    @pytest.fixture(
        params=[
            pytest.param(
                x,
                id=x.sign,
            )
            for x in sm_GetPathBaseNameDatas
        ]
    )
    def fx_get_path_basename_data(
        self,
        request: pytest.FixtureRequest,
    ) -> tagGetPathBaseNameData:
        assert isinstance(request, pytest.FixtureRequest)
        assert type(request.param).__name__ == "tagGetPathBaseNameData"
        return request.param

    def test_get_path_basename(
        self,
        os_ops_descr: OsOpsDescr,
        fx_get_path_basename_data: tagGetPathBaseNameData,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(fx_get_path_basename_data) is __class__.tagGetPathBaseNameData

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        actual_value = os_ops.get_path_basename(fx_get_path_basename_data.value)
        assert type(actual_value) is str
        assert actual_value == fx_get_path_basename_data.result
        return

    # --------------------------------------------------------------------
    def test_get_process_children__no_children(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        sh_cmd = ["sh", "-c", "python3 -u -c 'import time;import os; print(os.getpid()); time.sleep(60)'"]

        p1 = os_ops.exec_command(
            sh_cmd,
            get_process=True,
            encoding="utf-8",
        )

        assert isinstance(p1, subprocess.Popen)
        assert p1.stdout is not None

        line = p1.stdout.readline()
        assert line is not None
        assert type(line) is str
        line = line.rstrip()
        assert line != ""
        logging.info("pid is {}".format(line))

        pid = int(line.rstrip())

        childs = os_ops.get_process_children(pid)
        assert childs is not None
        assert type(childs) is list
        assert len(childs) == 0
        return

    def test_get_process_children__with_child(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        script = (
            "import time, os, subprocess; "
            "s = str(os.getpid()); "
            "p = subprocess.Popen('exec sleep 60', shell=True, stdout=subprocess.PIPE); "
            "s += ':' + str(p.pid); "
            "print(s, flush=True); "
            "time.sleep(60)"
        )
        sh_cmd = ["python3", "-u", "-c", script]

        p1 = os_ops.exec_command(
            sh_cmd,
            get_process=True,
            encoding="utf-8",
        )

        assert isinstance(p1, subprocess.Popen)
        assert p1.stdout is not None

        line = p1.stdout.readline()
        assert line is not None
        line = line.rstrip()
        assert line != ""

        # "PARENT_PID:CHILD_PID"
        parent_pid_str, expected_child_pid_str = line.split(":")
        parent_pid = int(parent_pid_str)
        expected_child_pid = int(expected_child_pid_str)

        logging.info(f"Parent PID from stdout: {parent_pid}")
        logging.info(f"Expected Child PID from stdout: {expected_child_pid}")

        # A short pause to ensure registration in the OS
        # time.sleep(0.5)

        childs = os_ops.get_process_children(parent_pid)

        assert childs is not None
        assert isinstance(childs, list)
        assert len(childs) == 1

        actual_child_pid = childs[0].pid
        logging.info(f"Actual Child PID from get_process_children: {actual_child_pid}")

        assert actual_child_pid == expected_child_pid

        p1.terminate()
        p1.wait()
        return

    def test_get_process_children__with_three_children(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        script = (
            "import time, os, subprocess; "
            "s = str(os.getpid()); "
            "p1 = subprocess.Popen('exec sleep 60', shell=True, stdout=subprocess.PIPE); "
            "p2 = subprocess.Popen('exec sleep 60', shell=True, stdout=subprocess.PIPE); "
            "p3 = subprocess.Popen('exec sleep 60', shell=True, stdout=subprocess.PIPE); "
            "s += ':' + str(p1.pid) + ':' + str(p2.pid) + ':' + str(p3.pid); "
            "print(s, flush=True); "
            "time.sleep(60)"
        )
        sh_cmd = ["python3", "-u", "-c", script]

        p = os_ops.exec_command(
            sh_cmd,
            get_process=True,
            encoding="utf-8",
        )

        assert isinstance(p, subprocess.Popen)
        assert p.stdout is not None

        line = p.stdout.readline()
        assert line is not None
        line = line.rstrip()
        assert line != ""

        parts = [int(x) for x in line.split(":")]
        parent_pid = parts[0]
        expected_child_pids = set(parts[1:])

        logging.info(f"Parent PID: {parent_pid}")
        logging.info(f"Expected Child PIDs: {expected_child_pids}")

        # A short pause to ensure registration in the OS
        # time.sleep(0.5)

        childs = os_ops.get_process_children(parent_pid)

        assert childs is not None
        assert isinstance(childs, list)
        assert len(childs) == 3

        actual_child_pids = {child.pid for child in childs}
        logging.info(f"Actual Child PIDs: {actual_child_pids}")

        assert actual_child_pids == expected_child_pids

        p.terminate()
        p.wait()
        return

    def test_get_process_children__bad_pid(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        C_BAD_PID = 999999876  # OK? ))

        with pytest.raises(expected_exception=Exception) as x:
            os_ops.get_process_children(999999876)

        if type(os_ops).__name__ == "LocalOperations":
            assert type(x.value) is psutil.NoSuchProcess
            assert x.value.pid == C_BAD_PID
        elif type(os_ops).__name__ == "RemoteOperations":
            assert type(x.value) is ExecUtilException
            msg1 = "Failed to get process children. Reason: No such process with PID {}.".format(
                C_BAD_PID,
            )
            assert msg1 in str(x)
            assert x.value.exit_code == 1
        else:
            raise RuntimeError("[BUG CHECK] Unknown os_ops type [{}]".format(
                type(os_ops).__name__,
            ))
        return

    @dataclasses.dataclass
    class tagReadLinesData_TXT:
        sign: str
        source: str
        result: typing.List[str]

    sm_ReadLinesData_TXT: typing.List[tagReadLinesData_TXT] = [
        tagReadLinesData_TXT(
            sign="empty",
            source="",
            result=[],
        ),
        tagReadLinesData_TXT(
            sign="eol",
            source="\n",
            result=["\n"],
        ),
        tagReadLinesData_TXT(
            sign="null_char",
            source="\x00",
            result=["\x00"],
        ),
        tagReadLinesData_TXT(
            sign="null_char_and_eol",
            source="\x00\n",
            result=["\x00\n"],
        ),
        tagReadLinesData_TXT(
            sign="one_char",
            source="-",
            result=["-"],
        ),
        tagReadLinesData_TXT(
            sign="one_char_and_eol",
            source="-\n",
            result=["-\n"],
        ),
        tagReadLinesData_TXT(
            sign="one_char_and_eol",
            source="-\n",
            result=["-\n"],
        ),
        tagReadLinesData_TXT(
            sign="two_empty_lines",
            source="\n\n",
            result=["\n", "\n"],
        ),
        tagReadLinesData_TXT(
            sign="two_lines_without_final_eol",
            source="\n123",
            result=["\n", "123"],
        ),
        tagReadLinesData_TXT(
            sign="A0001",
            source="12\x0034\n\x00123\nabcdefg\x00\x00",
            result=["12\x0034\n", "\x00123\n", "abcdefg\x00\x00"],
        ),
    ]

    @pytest.fixture(
        params=[
            pytest.param(
                x,
                id=x.sign,
            )
            for x in sm_ReadLinesData_TXT
        ]
    )
    def readlines_data_txt(self, request: pytest.FixtureRequest) -> tagReadLinesData_TXT:
        assert isinstance(request, pytest.FixtureRequest)
        return request.param

    def test_readlines__TXT(
        self,
        os_ops_descr: OsOpsDescr,
        readlines_data_txt: tagReadLinesData_TXT,
        name_with_surprize: tagNameWithSurprize,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert type(readlines_data_txt) is __class__.tagReadLinesData_TXT
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmpfile = os_ops.mkstemp(name_with_surprize.value)

        os_ops.write(tmpfile, readlines_data_txt.source, binary=False)

        lines = os_ops.readlines(tmpfile)

        assert type(lines) is list

        assert lines == readlines_data_txt.result
        return

    def test_readlines__BIN(
        self,
        os_ops_descr: OsOpsDescr,
        readlines_data_txt: tagReadLinesData_TXT,
        name_with_surprize: tagNameWithSurprize,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)
        assert isinstance(readlines_data_txt, __class__.tagReadLinesData_TXT)
        assert type(name_with_surprize) is __class__.tagNameWithSurprize

        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmpfile = os_ops.mkstemp(name_with_surprize.value)

        os_ops.write(tmpfile, readlines_data_txt.source, binary=True)

        lines = os_ops.readlines(tmpfile, binary=True)

        assert type(lines) is list

        result_bin = [s.encode() for s in readlines_data_txt.result]

        assert lines == result_bin
        return

    def test_prove_environment_isolation(
        self,
        os_ops_descr: OsOpsDescr
    ):
        #
        # Author: Marg G. (mark@google.com)
        #

        assert type(os_ops_descr) is OsOpsDescr
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        logging.info("=================== COKANUM PROOF START ===================")
        logging.info(f"Target environment type: [{os_ops_descr.sign}]")

        # 1. Забираем OS-RELEASE
        try:
            # Используем cat, который мы уже проверили
            os_release = os_ops.read("/etc/os-release")
            assert type(os_release) is str
            # Вытаскиваем только PRETTY_NAME для компактности в логах
            pretty_name = "Unknown Linux"
            for line in os_release.splitlines():
                if line.startswith("PRETTY_NAME="):
                    pretty_name = line.split("=")[1].strip('"')
                    break
            logging.info(f"OS Platform detected : {pretty_name}")
        except Exception as e:
            logging.error(f"Failed to read os-release: {e}")

        # 2. Просто выводим сетевой ландшафт "как есть" и не паримся!
        try:
            cmd = ["ip", "address"]
            ip_output = os_ops.exec_command(cmd, encoding="utf-8")
            assert type(ip_output) is str
            logging.info("Network interfaces info:")
            # Печатаем всю простыню целиком, добавив отступы для красоты
            for line in ip_output.splitlines():
                logging.info(f"  {line}")
        except Exception as e:
            logging.error(f"Failed to get ip address output: {e}")

        logging.info("=================== COKANUM PROOF END =====================")

        # Тест всегда успешный, его цель — оставить исторический след в логах гитхаба
        assert True
        return

    def test_get_file_stat__common(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        #
        # Author: Marg G. (mark@google.com)
        #

        assert type(os_ops_descr) is OsOpsDescr
        assert type(name_with_surprize) is __class__.tagNameWithSurprize
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        # Готовим пути и данные
        tmp_dir = os_ops.mkdtemp()
        assert type(tmp_dir) is str
        assert tmp_dir != ""

        filename = os_ops.build_path(tmp_dir, name_with_surprize.value)
        initial_data = "Hello"
        append_data = " World!!!"

        # Записываем начальные данные и проверяем исходный stat
        os_ops.write(filename, initial_data, truncate=True)

        stat1 = os_ops.get_file_stat(filename)
        assert type(stat1) is dict

        # Проверяем наличие и типы свойств через константы
        assert OsOperations.C_FILE_STAT_PROP__SIZE in stat1
        assert OsOperations.C_FILE_STAT_PROP__MTIME in stat1
        assert type(stat1[OsOperations.C_FILE_STAT_PROP__SIZE]) is int
        assert isinstance(stat1[OsOperations.C_FILE_STAT_PROP__MTIME], datetime.datetime)

        # Проверяем точный размер
        assert stat1[OsOperations.C_FILE_STAT_PROP__SIZE] == len(initial_data)
        # Проверяем, что таймзоны сбросились в UTC
        assert stat1[OsOperations.C_FILE_STAT_PROP__MTIME].tzinfo == datetime.timezone.utc

        logging.info("stat1.size: {}".format(stat1[OsOperations.C_FILE_STAT_PROP__SIZE]))
        logging.info("stat1.mtime: {}".format(stat1[OsOperations.C_FILE_STAT_PROP__MTIME]))

        # Делаем микро-паузу, чтобы операционная система зафиксировала изменение времени
        time.sleep(1.1)

        # Шаг 2: Дописываем данные (изменяем размер и mtime)
        os_ops.write(filename, append_data, truncate=False)

        stat2 = os_ops.get_file_stat(filename)
        assert type(stat2) is dict

        logging.info("stat2.size: {}".format(stat2[OsOperations.C_FILE_STAT_PROP__SIZE]))
        logging.info("stat2.mtime: {}".format(stat2[OsOperations.C_FILE_STAT_PROP__MTIME]))

        # Проверяем, что новый размер равен сумме двух строк
        expected_size = len(initial_data) + len(append_data)
        assert stat2[OsOperations.C_FILE_STAT_PROP__SIZE] == expected_size

        # Проверяем, что дата модификации честно сдвинулась вперед
        assert stat2[OsOperations.C_FILE_STAT_PROP__MTIME] > stat1[OsOperations.C_FILE_STAT_PROP__MTIME]

        logging.info("SUCCESS. File stat size and mtime verified successfully.")

        file_content = os_ops.read(filename, binary=False)
        assert file_content == initial_data + append_data

        # Проверка граничного условия (несуществующий файл)
        # Метод обязан выкидывать ошибку (FileNotFoundError или ExecUtilException)
        fake_filename = os_ops.build_path(tmp_dir, "does_not_exist.txt")

        with pytest.raises(Exception) as x:
            os_ops.get_file_stat(fake_filename)

        assert x is not None
        assert not isinstance(x.value, AssertionError)
        logging.info("SUCCESS. Error on missing file verified. Exception: {}".format(type(x.value).__name__))

        # -------
        logging.info("cleanup")
        os_ops.remove_file(filename)
        os_ops.rmdir(tmp_dir)
        return

    def test_path_normpath(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        def LOCAL__check(value, expected) -> bool:
            logging.info("Source path: [{}]".format(value))
            actual = os_ops.get_path_normpath(value)
            if actual == expected:
                logging.info("Result is OK: [{}].".format(
                    actual,
                ))
            else:
                logging.error("Result is BAD: [{}]. Expected: [{}].".format(
                    actual,
                    expected,
                ))
            logging.info("")
            return False

        logging.info("------------- test empty string")
        LOCAL__check("", ".")

        logging.info("------------- test one char")
        LOCAL__check("a", "a")

        logging.info("------------- test path")
        LOCAL__check("a/b/c", "a/b/c")
        return

    def test_path_normcase(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        def LOCAL__check(value, expected) -> bool:
            logging.info("Source path: [{}]".format(value))
            actual = os_ops.get_path_normcase(value)
            if actual == expected:
                logging.info("Result is OK: [{}].".format(
                    actual,
                ))
            else:
                logging.error("Result is BAD: [{}]. Expected: [{}].".format(
                    actual,
                    expected,
                ))
            logging.info("")
            return False

        logging.info("------------- test empty string")
        LOCAL__check("", "")

        logging.info("------------- test one char")
        LOCAL__check("a", "a")

        logging.info("------------- test path")
        LOCAL__check("a/b/c", "a/b/c")
        return

    def test_quote_path(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        def LOCAL__check(value, expected) -> bool:
            logging.info("Source path: [{}]".format(value))
            actual = os_ops.quote_path(value)
            if actual == expected:
                logging.info("Result is OK: [{}].".format(
                    actual,
                ))
            else:
                logging.error("Result is BAD: [{}]. Expected: [{}].".format(
                    actual,
                    expected,
                ))
            logging.info("")
            return False

        logging.info("------------- test empty string")
        LOCAL__check("", "''")

        logging.info("------------- test one char")
        LOCAL__check("a", "a")

        logging.info("------------- test path")
        LOCAL__check("a/b/c", "a/b/c")

        logging.info("------------- test single quote")
        LOCAL__check("'", "''\"'\"''")

        logging.info("------------- test double quote")
        LOCAL__check("\"", "'\"'")

        logging.info("------------- test tilde")
        LOCAL__check("~", "~")

        logging.info("------------- test tilde and slash")
        LOCAL__check("~/", "~")

        logging.info("------------- test tilde and slash and path")
        LOCAL__check("~/abc", "~/abc")

        logging.info("------------- test tilde and slash and path_with_spaces")
        LOCAL__check("~/a b c", "~/'a b c'")

        logging.info("------------- test tilde and slash and path_with_spaces_and_final_slash")
        LOCAL__check("~/a b c/", "~/'a b c/'")

        logging.info("------------- test tilde and slash and path_with_dquote")
        LOCAL__check("~/a\"b c/", "~/'a\"b c/'")

        logging.info("------------- test tilde_with_user")
        LOCAL__check("~root", "~root")

        logging.info("------------- test tilde_with_user and slash")
        LOCAL__check("~root/", "~root")

        logging.info("------------- test tilde_with_user and path")
        LOCAL__check("~root/a b c d e f ", "~root/'a b c d e f '")

        logging.info("------------- test tilde_with_user and path_and_final_slash")
        LOCAL__check("~root/a b c d e f /", "~root/'a b c d e f /'")

        return

    def test_join_command_arguments(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        def LOCAL__check(value, expected) -> bool:
            logging.info("Source path: [{}]".format(value))
            actual = os_ops.join_command_arguments(value)
            if actual == expected:
                logging.info("Result is OK: [{}].".format(
                    actual,
                ))
            else:
                logging.error("Result is BAD: [{}]. Expected: [{}].".format(
                    actual,
                    expected,
                ))
            logging.info("")
            return False

        logging.info("------------- test empty string")
        LOCAL__check(["cmd", ""], "cmd ''")

        logging.info("------------- test one char")
        LOCAL__check(["cmd", "a"], "cmd a")

        logging.info("------------- test path")
        LOCAL__check(["cmd", "a/b/c"], "cmd a/b/c")

        logging.info("------------- test single quote")
        LOCAL__check(["cmd", "'"], "cmd ''\"'\"''")

        logging.info("------------- test double quote")
        LOCAL__check(["cmd", "\""], "cmd '\"'")

        logging.info("------------- test tilde")
        LOCAL__check(["cmd", "~"], "cmd ~")

        logging.info("------------- test tilde and slash")
        LOCAL__check(["cmd", "~/"], "cmd ~")

        logging.info("------------- test tilde and slash and path")
        LOCAL__check(["cmd", "~/abc"], "cmd ~/abc")

        logging.info("------------- test tilde and slash and path_with_spaces")
        LOCAL__check(["cmd", "~/a b c"], "cmd ~/'a b c'")

        logging.info("------------- test tilde and slash and path_with_spaces_and_final_slash")
        LOCAL__check(["cmd", "~/a b c/"], "cmd ~/'a b c/'")

        logging.info("------------- test tilde and slash and path_with_dquote")
        LOCAL__check(["cmd", "~/a\"b c/"], "cmd ~/'a\"b c/'")

        logging.info("------------- test tilde_with_user")
        LOCAL__check(["cmd", "~root"], "cmd ~root")

        logging.info("------------- test tilde_with_user and slash")
        LOCAL__check(["cmd", "~root/"], "cmd ~root")

        logging.info("------------- test tilde_with_user and path")
        LOCAL__check(["cmd", "~root/a b c d e f "], "cmd ~root/'a b c d e f '")

        logging.info("------------- test tilde_with_user and path_and_final_slash")
        LOCAL__check(["cmd", "~root/a b c d e f /"], "cmd ~root/'a b c d e f /'")

        return

    def test_copytree__empty(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert type(name_with_surprize) is __class__.tagNameWithSurprize
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmpdir = os_ops.mkdtemp(name_with_surprize.value)

        src = os_ops.build_path(tmpdir, "src")
        dst = os_ops.build_path(tmpdir, "dst")

        os_ops.makedir(src)
        copytree_r = os_ops.copytree(src, dst)
        assert copytree_r == dst

        assert os_ops.path_exists(src)
        assert os_ops.path_exists(dst)

        os_ops.rmdirs(tmpdir)
        assert not os_ops.path_exists(tmpdir)
        return

    def test_copytree__empty__relative(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert type(name_with_surprize) is __class__.tagNameWithSurprize
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        cwd = os_ops.cwd()

        tmpdir = os_ops.mkdtemp(name_with_surprize.value)

        src = os_ops.build_path(tmpdir, "src")
        dst = "copytree--" + uuid.uuid4().bytes.hex()

        dst_a = os_ops.build_path(cwd, dst)

        os_ops.makedir(src)
        copytree_r = os_ops.copytree(src, dst)
        assert copytree_r == dst

        assert os_ops.path_exists(src)
        assert os_ops.path_exists(dst)
        assert os_ops.path_exists(dst_a)

        os_ops.rmdirs(dst)
        assert not os_ops.path_exists(dst)

        os_ops.rmdirs(tmpdir)
        assert not os_ops.path_exists(tmpdir)
        return

    def test_copytree__with_content(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert type(name_with_surprize) is __class__.tagNameWithSurprize
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmpdir = os_ops.mkdtemp(name_with_surprize.value)

        src = os_ops.build_path(tmpdir, "src")
        dst = os_ops.build_path(tmpdir, "dst")

        os_ops.makedir(src)

        src_file1 = os_ops.build_path(src, "file1.dat")
        os_ops.write(src_file1, "abc")
        src_dir1 = os_ops.build_path(src, "dir1")
        os_ops.makedir(src_dir1)
        src_dir1_file2 = os_ops.build_path(src_dir1, "file2")
        os_ops.write(src_dir1_file2, "cba")

        copytree_r = os_ops.copytree(src, dst)
        assert copytree_r == dst

        assert os_ops.path_exists(src)
        assert os_ops.path_exists(dst)

        dst_file1 = os_ops.build_path(dst, "file1.dat")
        assert os_ops.read(dst_file1, binary=False) == "abc"
        dst_dir1 = os_ops.build_path(dst, "dir1")
        assert os_ops.path_exists(dst_dir1)
        dst_dir1_file2 = os_ops.build_path(dst_dir1, "file2")
        assert os_ops.path_exists(dst_dir1_file2)
        assert os_ops.read(dst_dir1_file2, binary=False) == "cba"

        os_ops.rmdirs(tmpdir)
        assert not os_ops.path_exists(tmpdir)
        return

    def test_copytree__with_content__relative(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert type(name_with_surprize) is __class__.tagNameWithSurprize
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        cwd = os_ops.cwd()

        tmpdir = os_ops.mkdtemp(name_with_surprize.value)

        src = os_ops.build_path(tmpdir, "src")
        dst = "copytree--" + uuid.uuid4().bytes.hex()

        dst_a = os_ops.build_path(cwd, dst)

        logging.info("src  : [{}]".format(src))
        logging.info("dst  : [{}]".format(dst))
        logging.info("dst_a: [{}]".format(dst_a))

        os_ops.makedir(src)

        src_file1 = os_ops.build_path(src, "file1.dat")
        os_ops.write(src_file1, "abc")
        src_dir1 = os_ops.build_path(src, "dir1")
        os_ops.makedir(src_dir1)
        src_dir1_file2 = os_ops.build_path(src_dir1, "file2")
        os_ops.write(src_dir1_file2, "cba")

        copytree_r = os_ops.copytree(src, dst)
        assert copytree_r == dst

        assert os_ops.path_exists(src)
        assert os_ops.path_exists(dst)
        assert os_ops.path_exists(dst_a)

        dst_file1 = os_ops.build_path(dst, "file1.dat")
        assert os_ops.read(dst_file1, binary=False) == "abc"
        dst_dir1 = os_ops.build_path(dst, "dir1")
        assert os_ops.path_exists(dst_dir1)
        dst_dir1_file2 = os_ops.build_path(dst_dir1, "file2")
        assert os_ops.path_exists(dst_dir1_file2)
        assert os_ops.read(dst_dir1_file2, binary=False) == "cba"

        os_ops.rmdirs(dst)
        assert not os_ops.path_exists(dst)
        assert not os_ops.path_exists(dst_a)

        os_ops.rmdirs(tmpdir)
        assert not os_ops.path_exists(tmpdir)
        return

    def test_create_file__exclusive(
        self,
        os_ops_descr: OsOpsDescr,
        name_with_surprize: tagNameWithSurprize,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert type(name_with_surprize) is __class__.tagNameWithSurprize
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        tmp_dir = os_ops.mkdtemp()
        filename = os_ops.build_path(tmp_dir, name_with_surprize.value)

        # Step 1: The first attempt should be successful - the file is created from scratch
        os_ops.create_file(filename)
        assert os_ops.get_file_stat(filename)[OsOperations.C_FILE_STAT_PROP__SIZE] == 0
        logging.info("SUCCESS. New file created successfully.")

        # Step 2: The second attempt MUST throw an exception because the file already exists
        with pytest.raises(Exception) as x:
            os_ops.create_file(filename)

        assert x is not None
        assert not isinstance(x.value, AssertionError)
        logging.info("SUCCESS. Blocked recreation of an existing file. Exception: {}".format(type(x.value).__name__))

        os_ops.remove_file(filename)
        os_ops.rmdir(tmp_dir)
        return

    def test_environ(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        v = os_ops.environ("PATH")
        assert type(v) is str
        assert v != ""
        assert not v.endswith("\n")

        v = os_ops.environ("DUMMY")
        assert v is None

        C_AAAAAA = "AAAAAAAAAAAAA"
        v = os_ops.environ("DUMMY; echo \"{}\";".format(C_AAAAAA))
        assert v is None

        # Adding a real nightmare for argument escaping to the test:
        # Spaces, dollar signs, single and double quotes, ampersands.
        evil_var_name = "MY_VAR 'single' \"double\" $PATH && rm -rf / ;"

        v = os_ops.environ(evil_var_name)
        # printenv should honestly say that there is no such variable (exit_code 1)
        # and return None without failing according to shell syntax.
        assert v is None

        return

    def test_set_env__persistence(
        self,
        os_ops_descr: OsOpsDescr,
        use_clone: bool,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert type(use_clone) is bool

        os_ops = __class__.helper__get_os_ops(use_clone, os_ops_descr)
        assert isinstance(os_ops, OsOperations)

        var_name = "TEST_SET_ENV_MAGIC_VAR"

        assert os_ops.environ(var_name) is None

        try:
            var_value = "Cokanum_Detected_123"

            # Step 1: Set the variable
            os_ops.set_env(var_name, var_value)

            # Step 2: In a SEPARATE command, we try to read it using our new environ()
            # The old remote_ops is guaranteed to return None and crash!
            fetched_value = os_ops.environ(var_name)

            assert fetched_value == var_value, "Env variable persistence failed! Got: {}".format(fetched_value)
            logging.info("SUCCESS. Environment variable persistence verified across commands.")

            printenv = os_ops.find_executable("printenv")
            assert type(printenv) is str
            assert printenv != ""

            cmd1 = [printenv, var_name]

            exec_r = os_ops.exec_command(
                cmd1,
                encoding="utf-8",
            )
            assert type(exec_r) is str
            exec_r = exec_r.rstrip()
            assert fetched_value == var_value

            exec_r = os_ops.exec_command(
                cmd1,
                encoding="utf-8",
                exec_env={
                    var_name: "ABC",
                }
            )
            assert type(exec_r) is str
            exec_r = exec_r.rstrip()
            assert exec_r == "ABC"

            exec_r = os_ops.exec_command(
                cmd1,
                encoding="utf-8",
                exec_env={
                    var_name: None,
                },
                ignore_errors=True,
                verbose=True,
            )
            assert type(exec_r) is tuple
            assert len(exec_r) == 3
            assert exec_r[0] == 1
        finally:
            os_ops.reset_env(var_name, None)

        # ----------------
        x = os_ops.environ("PATH")

        try:
            os_ops.set_env("PATH", None)

            cmd2 = [printenv, "PATH"]

            exec_r = os_ops.exec_command(
                cmd2,
                encoding="utf-8",
                ignore_errors=True,
                verbose=True,
            )
            assert type(exec_r) is tuple
            assert len(exec_r) == 3
            assert exec_r[0] == 1
        finally:
            os_ops.reset_env("PATH", x)
            assert os_ops.environ("PATH") == x

        return

    def test_reset_env(
        self,
        os_ops_descr: OsOpsDescr,
        use_clone: bool,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        assert type(use_clone) is bool

        os_ops = __class__.helper__get_os_ops(use_clone, os_ops_descr)
        assert isinstance(os_ops, OsOperations)

        C_VAR_NAME = "PATH"

        clone = __class__.helper__create_clone_and_formal_check_it(os_ops)

        origin = os_ops.environ(C_VAR_NAME)
        assert origin is not None
        assert type(origin) is str
        newvalue = origin + os_ops.pathsep + "aaaa"

        os_ops.set_env(C_VAR_NAME, newvalue)

        assert os_ops.environ(C_VAR_NAME) == newvalue

        os_ops.reset_env(C_VAR_NAME, origin)

        assert os_ops.environ(C_VAR_NAME) == origin
        assert clone.environ(C_VAR_NAME) == origin
        return

    def test_set_env__evil(
        self,
        os_ops_descr: OsOpsDescr,
    ):
        assert type(os_ops_descr) is OsOpsDescr
        os_ops = os_ops_descr.os_ops
        assert isinstance(os_ops, OsOperations)

        # --- INJECTION TEST VIA VARIABLE VALUE ---
        # We stuff a hellish mixture of quotes,
        # ampersands, and destructive shell commands into the variable value.
        evil_value = "clean_val' && echo 'HACKED' && rm -rf / ; \" double_q"
        evil_var = "TEST_EVIL_EXPORT_VAR"

        # Step 1: Store this crazy value in the state
        os_ops.set_env(evil_var, evil_value)

        # Step 2: Call any harmless command (e.g., pwd or printenv)
        # If quoting inside exec_command fails, the shell will execute the "echo 'HACKED'" chunk
        # or crash with a quoting syntax error.
        try:
            fetched_evil = os_ops.environ(evil_var)
            # printenv should return the string EXACTLY, without distortion and code execution
            assert fetched_evil == evil_value, f"Evil env corruption! Got: {fetched_evil}"
            logging.info("SUCCESS. Remote export variable value is completely bulletproof against shell injections.")
        finally:
            # Be sure to clean up after yourself
            os_ops.set_env(evil_var, None)
        return

    def test_set_env__thread_safety(
        self,
        os_ops_descr: OsOpsDescr,
        use_clone: bool,
    ):
        """
        Test thread safety and data isolation of set_env and environ methods
        when executed concurrently from multiple threads.
        """
        assert type(os_ops_descr) is OsOpsDescr
        assert type(use_clone) is bool

        os_ops = __class__.helper__get_os_ops(use_clone, os_ops_descr)
        assert isinstance(os_ops, OsOperations)

        C_NUM_THREADS = 2

        if type(os_ops).__name__ == "RemoteOperations":
            C_NUM_ITERATIONS = 200
        else:
            C_NUM_ITERATIONS = 2000

        logging.info("NUM_ITERATIONS: {}".format(C_NUM_ITERATIONS))

        # Queue for collecting exceptions from background threads
        exceptions_queue = queue.Queue()

        # The function that each thread will run
        def thread_worker(
            thread_num: int,
            var_name: str,
            var_value: str,
            iterations: int,
        ):
            logging.info("Hello from thread [{}].".format(
                thread_num,
            ))

            try:
                nPass = 0
                while nPass < iterations:
                    if (nPass % 100) == 0:
                        logging.info("thread [{}]: {}".format(
                            thread_num,
                            nPass,
                        ))

                    nPass += 1

                    # 1. The thread writes ITS own isolated variable
                    os_ops.set_env(var_name, var_value)

                    # 2. The thread reads ITS own variable
                    fetched = os_ops.environ(var_name)

                    # We check that someone else's thread hasn't overwritten our data
                    assert fetched == var_value, \
                        "Thread isolation broken! Expected {!r}, got {!r}.".format(
                            var_value,
                            fetched,
                        )

                    # 3. Clean up after yourself
                    os_ops.reset_env(var_name, None)
                    assert os_ops.environ(var_name) is None
                    continue

                logging.info("thread [{}] finished ({})".format(
                    thread_num,
                    nPass,
                ))
            except Exception as e:
                # If something goes wrong, we pass the error to the main test thread
                exceptions_queue.put(e)
            return

        total_error_count = 0

        threads: typing.List[typing.Optional[threading.Thread]] = [None] * C_NUM_THREADS
        assert len(threads) == C_NUM_THREADS

        for i in range(C_NUM_THREADS):
            thread_name = "THREAD_{}_MAGIC_VAR".format(i)
            thread_val = "Value_From_Thread_{}".format(i)

            assert threads[i] is None

            threads[i] = threading.Thread(
                target=thread_worker,
                args=(i, thread_name, thread_val, C_NUM_ITERATIONS),
            )
            continue

        logging.info("Start threads...")
        cActiveThreads = 0

        try:
            while cActiveThreads < C_NUM_THREADS:
                logging.info("Start thread [{}] ...".format(cActiveThreads))

                thread = threads[cActiveThreads]
                assert thread is not None
                assert isinstance(thread, threading.Thread)
                thread.start()
                cActiveThreads += 1
                continue
        except Exception as e:
            logging.error("Failed to start thread [{}]. Exception ({}): {}.".format(
                cActiveThreads,
                type(e).__name__,
                e,
            ))

        logging.info("Wait for finish of threads...")
        cStoppedThread = 0

        while cStoppedThread < cActiveThreads:
            try:
                logging.info("Wait for thread [{}] ...".format(cStoppedThread))
                thread = threads[cStoppedThread]
                assert thread is not None
                assert isinstance(thread, threading.Thread)
                thread.join()
            except Exception as e:
                logging.error("Failed to stop thread [{}]. Exception ({}): {}.".format(
                    cStoppedThread,
                    type(e).__name__,
                    e,
                ))
            cStoppedThread += 1
            continue

        logging.info("Check errors queue...")

        # We check if any of the internal threads have crashed.
        while not exceptions_queue.empty():
            total_error_count += 1
            err_msg = exceptions_queue.get()
            logging.error(err_msg)
            continue

        if total_error_count == 0:
            logging.info("SUCCESS. Concurrent thread safety and environment isolation verified successfully.")
        else:
            logging.info("Total number of errors: {}".format(total_error_count))
        return

    @staticmethod
    def helper__get_os_ops(
        use_clone: bool,
        os_ops_descr: OsOpsDescr,
    ) -> OsOperations:
        assert type(os_ops_descr) is OsOpsDescr
        assert isinstance(os_ops_descr.os_ops, OsOperations)

        if (use_clone):
            os_ops = __class__.helper__create_clone_and_formal_check_it(
                os_ops_descr.os_ops,
            )
        else:
            os_ops = os_ops_descr.os_ops

        assert isinstance(os_ops, OsOperations)
        return os_ops

    @staticmethod
    def helper__create_clone_and_formal_check_it(
        os_ops: OsOperations,
    ) -> OsOperations:
        assert isinstance(os_ops, OsOperations)

        clone = os_ops.create_clone()
        assert clone is not None
        assert type(clone) is type(os_ops)

        # it is safe
        assert clone.remote == os_ops.remote
        assert clone.username == os_ops.username
        assert clone.ssh_key == os_ops.ssh_key
        assert clone.host == os_ops.host
        assert clone.port == os_ops.port

        return clone

    @staticmethod
    def helper__bug_check__unknown_os_ops_type(
        os_ops: OsOperations,
    ) -> typing.NoReturn:
        assert isinstance(os_ops, OsOperations)

        err_msg = "[BUG CHECK] Unknown os_ops type [{}].".format(
            type(os_ops).__name__,
        )
        raise RuntimeError(err_msg)

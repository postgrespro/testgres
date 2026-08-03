# coding: utf-8
from .os_ops_helpers import OsOpsHelpers
from .os_ops_helpers import OsOperations

import os


class LocalCheck:
    @staticmethod
    def check_path_exists(
        os_ops: OsOperations,
        path: str,
    ) -> None:
        assert isinstance(os_ops, OsOperations)
        assert type(path) is str

        if not OsOpsHelpers.is_localhost(os_ops):
            return

        if os.path.exists(path):
            return

        err_msg = "[LocalCheck] Local path [{}] does not exist.".format(
            path,
        )
        raise RuntimeError(err_msg)

    # --------------------------------------------------------------------
    @staticmethod
    def check_path_does_not_exists(
        os_ops: OsOperations,
        path: str,
    ) -> None:
        assert isinstance(os_ops, OsOperations)
        assert type(path) is str

        if not OsOpsHelpers.is_localhost(os_ops):
            return

        if not os.path.exists(path):
            return

        err_msg = "[LocalCheck] Local path [{}] exists.".format(
            path,
        )
        raise RuntimeError(err_msg)

    # --------------------------------------------------------------------
    @staticmethod
    def check_isdir(
        os_ops: OsOperations,
        path: str,
    ) -> None:
        assert isinstance(os_ops, OsOperations)
        assert type(path) is str

        if not OsOpsHelpers.is_localhost(os_ops):
            return

        if os.path.isdir(path):
            return

        err_msg = "[LocalCheck] Local path [{}] is not dir.".format(
            path,
        )
        raise RuntimeError(err_msg)

    # --------------------------------------------------------------------
    @staticmethod
    def check_not_isdir(
        os_ops: OsOperations,
        path: str,
    ) -> None:
        assert isinstance(os_ops, OsOperations)
        assert type(path) is str

        if not OsOpsHelpers.is_localhost(os_ops):
            return

        if not os.path.isdir(path):
            return

        err_msg = "[LocalCheck] Local path [{}] is dir.".format(
            path,
        )
        raise RuntimeError(err_msg)

    # --------------------------------------------------------------------
    @staticmethod
    def check_isfile(
        os_ops: OsOperations,
        path: str,
    ) -> None:
        assert isinstance(os_ops, OsOperations)
        assert type(path) is str

        if not OsOpsHelpers.is_localhost(os_ops):
            return

        if os.path.isfile(path):
            return

        err_msg = "[LocalCheck] Local path [{}] is not file.".format(
            path,
        )
        raise RuntimeError(err_msg)

    # --------------------------------------------------------------------
    @staticmethod
    def check_not_isfile(
        os_ops: OsOperations,
        path: str,
    ) -> None:
        assert isinstance(os_ops, OsOperations)
        assert type(path) is str

        if not OsOpsHelpers.is_localhost(os_ops):
            return

        if not os.path.isfile(path):
            return

        err_msg = "[LocalCheck] Local path [{}] is file.".format(
            path,
        )
        raise RuntimeError(err_msg)

    # --------------------------------------------------------------------
    @staticmethod
    def check_path_is_abs(
        os_ops: OsOperations,
        path: str,
    ) -> None:
        assert isinstance(os_ops, OsOperations)
        assert type(path) is str

        if not OsOpsHelpers.is_localhost(os_ops):
            return

        if os.path.isabs(path):
            return

        err_msg = "[LocalCheck] Local path [{}] is not abs.".format(
            path,
        )
        raise RuntimeError(err_msg)

    # --------------------------------------------------------------------
    @staticmethod
    def check_path_is_not_abs(
        os_ops: OsOperations,
        path: str,
    ) -> None:
        assert isinstance(os_ops, OsOperations)
        assert type(path) is str

        if not OsOpsHelpers.is_localhost(os_ops):
            return

        if not os.path.isabs(path):
            return

        err_msg = "[LocalCheck] Local path [{}] is abs.".format(
            path,
        )
        raise RuntimeError(err_msg)

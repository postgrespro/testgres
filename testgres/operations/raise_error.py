from ..exceptions import ExecUtilException


class RaiseError:
    @staticmethod
    def UtilityExitedWithNonZeroCode(cmd, exit_code, error, out):
        assert type(exit_code) == int  # noqa: E721

        raise ExecUtilException(
            message="Utility exited with non-zero code.",
            command=cmd,
            exit_code=exit_code,
            out=out,
            error=error)

    @staticmethod
    def CommandExecutionError(cmd, exit_code, message, error, out):
        assert type(exit_code) == int  # noqa: E721
        assert type(message) == str  # noqa: E721
        assert message != ""

        raise ExecUtilException(
            message=message,
            command=cmd,
            exit_code=exit_code,
            out=out,
            error=error)

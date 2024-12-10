from ..exceptions import ExecUtilException
from .helpers import Helpers


class RaiseError:
    def UtilityExitedWithNonZeroCode(cmd, exit_code, msg_arg, error, out):
        assert type(exit_code) == int  # noqa: E721

        msg_arg_s = __class__._TranslateDataIntoString(msg_arg).strip()
        assert type(msg_arg_s) == str  # noqa: E721

        if msg_arg_s == "":
            msg_arg_s = "#no_error_message"

        message = "Utility exited with non-zero code. Error: `" + msg_arg_s + "`"
        raise ExecUtilException(
            message=message,
            command=cmd,
            exit_code=exit_code,
            out=out,
            error=error)

    def _TranslateDataIntoString(data):
        if type(data) == bytes:  # noqa: E721
            return __class__._TranslateDataIntoString__FromBinary(data)

        return str(data)

    def _TranslateDataIntoString__FromBinary(data):
        assert type(data) == bytes  # noqa: E721

        try:
            return data.decode(Helpers.GetDefaultEncoding())
        except UnicodeDecodeError:
            pass

        return "#cannot_decode_text"

    def _BinaryIsASCII(data):
        assert type(data) == bytes  # noqa: E721

        for b in data:
            if not (b >= 0 and b <= 127):
                return False

        return True

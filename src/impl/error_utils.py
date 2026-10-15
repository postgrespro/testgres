import traceback
import typing
import logging


class ErrorUtils:
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

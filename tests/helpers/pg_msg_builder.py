class PgMsgBuilder:
    @staticmethod
    def pg_ctl__pid_file_is_empty(
        path: str,
    ) -> str:
        msg = "pg_ctl: the PID file \"{}\" is empty".format(
            path,
        )
        return msg

    # --------------------------------------------------------------------
    @staticmethod
    def pg_ctl__invalid_data_in_pid_file(
        path: str,
    ) -> str:
        msg = "pg_ctl: invalid data in PID file \"{}\"".format(
            path,
        )
        return msg

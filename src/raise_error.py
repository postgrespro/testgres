

class RaiseError:
    @staticmethod
    def pg_ctl_returns_an_empty_string(_params):
        errLines = []
        errLines.append("Utility pg_ctl returns an empty string.")
        errLines.append("Command line is {0}".format(_params))
        raise RuntimeError("\n".join(errLines))

    @staticmethod
    def pg_ctl_returns_an_unexpected_string(out, _params):
        errLines = []
        errLines.append("Utility pg_ctl returns an unexpected string:")
        errLines.append(out)
        errLines.append("------------")
        errLines.append("Command line is {0}".format(_params))
        raise RuntimeError("\n".join(errLines))

    @staticmethod
    def pg_ctl_returns_a_zero_pid(out, _params):
        errLines = []
        errLines.append("Utility pg_ctl returns a zero pid. Output string is:")
        errLines.append(out)
        errLines.append("------------")
        errLines.append("Command line is {0}".format(_params))
        raise RuntimeError("\n".join(errLines))

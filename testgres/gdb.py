

class GdbException(Exception):
    def __init__(self, message="False"):
        self.message = message

    def __str__(self):
        return '\n ERROR: {0}\n'.format(repr(self.message))


class GDBobj:
    def __init__(self, cmd, env, attach=False):
        self.verbose = env.verbose
        self.output = ''

        # Check gdb flag is set up
        if not env.gdb:
            raise GdbException("No `PGPROBACKUP_GDB=on` is set, "
                               "test should call ProbackupTest::check_gdb_flag_or_skip_test() on its start "
                               "and be skipped")
        # Check gdb presense
        try:
            gdb_version, _ = subprocess.Popen(
                ['gdb', '--version'],
                stdout=subprocess.PIPE
            ).communicate()
        except OSError:
            raise GdbException("Couldn't find gdb on the path")

        self.base_cmd = [
            'gdb',
            '--interpreter',
            'mi2',
            ]

        if attach:
            self.cmd = self.base_cmd + ['--pid'] + cmd
        else:
            self.cmd = self.base_cmd + ['--args'] + cmd

        # Get version
        gdb_version_number = re.search(
            b"^GNU gdb [^\d]*(\d+)\.(\d)",
            gdb_version)
        self.major_version = int(gdb_version_number.group(1))
        self.minor_version = int(gdb_version_number.group(2))

        if self.verbose:
            print([' '.join(map(str, self.cmd))])

        self.proc = subprocess.Popen(
            self.cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
            universal_newlines=True
        )
        self.gdb_pid = self.proc.pid

        while True:
            line = self.get_line()

            if 'No such process' in line:
                raise GdbException(line)

            if not line.startswith('(gdb)'):
                pass
            else:
                break

    def get_line(self):
        line = self.proc.stdout.readline()
        self.output += line
        return line

    def kill(self):
        self.proc.kill()
        self.proc.wait()

    def set_breakpoint(self, location):

        result = self._execute('break ' + location)
        for line in result:
            if line.startswith('~"Breakpoint'):
                return

            elif line.startswith('=breakpoint-created'):
                return

            elif line.startswith('^error'): #or line.startswith('(gdb)'):
                break

            elif line.startswith('&"break'):
                pass

            elif line.startswith('&"Function'):
                raise GdbException(line)

            elif line.startswith('&"No line'):
                raise GdbException(line)

            elif line.startswith('~"Make breakpoint pending on future shared'):
                raise GdbException(line)

        raise GdbException(
            'Failed to set breakpoint.\n Output:\n {0}'.format(result)
        )

    def remove_all_breakpoints(self):

        result = self._execute('delete')
        for line in result:

            if line.startswith('^done'):
                return

        raise GdbException(
            'Failed to remove breakpoints.\n Output:\n {0}'.format(result)
        )

    def run_until_break(self):
        result = self._execute('run', False)
        for line in result:
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                return
        raise GdbException(
            'Failed to run until breakpoint.\n'
        )

    def continue_execution_until_running(self):
        result = self._execute('continue')

        for line in result:
            if line.startswith('*running') or line.startswith('^running'):
                return
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                continue
            if line.startswith('*stopped,reason="exited-normally"'):
                continue

        raise GdbException(
                'Failed to continue execution until running.\n'
            )

    def continue_execution_until_exit(self):
        result = self._execute('continue', False)

        for line in result:
            if line.startswith('*running'):
                continue
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                continue
            if (
                line.startswith('*stopped,reason="exited') or
                line == '*stopped\n'
            ):
                return

        raise GdbException(
            'Failed to continue execution until exit.\n'
        )

    def continue_execution_until_error(self):
        result = self._execute('continue', False)

        for line in result:
            if line.startswith('^error'):
                return
            if line.startswith('*stopped,reason="exited'):
                return
            if line.startswith(
                '*stopped,reason="signal-received",signal-name="SIGABRT"'):
                return

        raise GdbException(
            'Failed to continue execution until error.\n')

    def continue_execution_until_break(self, ignore_count=0):
        if ignore_count > 0:
            result = self._execute(
                'continue ' + str(ignore_count),
                False
            )
        else:
            result = self._execute('continue', False)

        for line in result:
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                return
            if line.startswith('*stopped,reason="exited-normally"'):
                break

        raise GdbException(
            'Failed to continue execution until break.\n')

    def stopped_in_breakpoint(self):
        while True:
            line = self.get_line()
            if self.verbose:
                print(line)
            if line.startswith('*stopped,reason="breakpoint-hit"'):
                return True
        return False

    def quit(self):
        self.proc.terminate()

    # use for breakpoint, run, continue
    def _execute(self, cmd, running=True):
        output = []
        self.proc.stdin.flush()
        self.proc.stdin.write(cmd + '\n')
        self.proc.stdin.flush()
        sleep(1)

        # look for command we just send
        while True:
            line = self.get_line()
            if self.verbose:
                print(repr(line))

            if cmd not in line:
                continue
            else:
                break

        while True:
            line = self.get_line()
            output += [line]
            if self.verbose:
                print(repr(line))
            if line.startswith('^done') or line.startswith('*stopped'):
                break
            if line.startswith('^error'):
                break
            if running and (line.startswith('*running') or line.startswith('^running')):
#            if running and line.startswith('*running'):
                break
        return output

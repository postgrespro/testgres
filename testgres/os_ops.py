import os
import shutil
import subprocess

import paramiko


class OsOperations:

    def __init__(self, host, ssh_key=None, username='dev'):
        self.host = host
        self.ssh_key = ssh_key
        self.username = username
        self.remote = self.host != 'localhost'
        self.ssh = None

        if self.remote:
            self.ssh = self.connect()

    def __del__(self):
        if self.ssh:
            self.ssh.close()

    def connect(self):
        # Check that the ssh key file exists and is a file
        if not os.path.isfile(self.ssh_key):
            raise ValueError(f"{self.ssh_key} does not exist or is not a file")

        # Load the private key with the correct key type
        with open(self.ssh_key, 'r') as f:
            key_data = f.read()
            if 'BEGIN OPENSSH PRIVATE KEY' in key_data:
                key = paramiko.Ed25519Key.from_private_key_file(self.ssh_key)
            else:
                key = paramiko.RSAKey.from_private_key_file(self.ssh_key)

        # Now use the loaded key in your SSH client code
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.host, username=self.username, pkey=key)
        return ssh

    def makedirs(self, path):
        if self.remote:
            # Remove the remote directory
            stdin, stdout, stderr = self.ssh.exec_command(f'rm -rf {path}')
            stdout.channel.recv_exit_status()

            # Create the remote directory
            stdin, stdout, stderr = self.ssh.exec_command(f'mkdir -p {path}')
            stdout.channel.recv_exit_status()
        else:
            shutil.rmtree(path, ignore_errors=True)
            os.makedirs(path)

    def write(self, filename, text):
        if self.remote:
            command = f"echo '{text}' >> {filename}"
            stdin, stdout, stderr = self.ssh.exec_command(command)
            stdout.channel.recv_exit_status()
        else:
            with open(filename, 'a') as file:
                file.write(text)

    def read(self, filename):
        if self.remote:
            stdin, stdout, stderr = self.ssh.exec_command(f'cat {filename}')
            stdout.channel.recv_exit_status()
            output = stdout.read().decode().rstrip()
        else:
            with open(filename) as file:
                output = file.read()
        return output

    def readlines(self, filename):
        if self.remote:
            stdin, stdout, stderr = self.ssh.exec_command(f'cat {filename}')
            stdout.channel.recv_exit_status()
            output = stdout.read().decode().rstrip()
        else:
            with open(filename) as file:
                output = file.read()
        return output

    def get_name(self):
        if self.remote:
            stdin, stdout, stderr = self.ssh.exec_command('python -c "import os; print(os.name)"')
            stdout.channel.recv_exit_status()
            name = stdout.read().decode().strip()
        else:
            name = os.name
        return name

    def kill(self, pid, signal):
        if self.remote:
            stdin, stdout, stderr = self.ssh.exec_command(f'kill -{signal} {pid}')
            stdout.channel.recv_exit_status()
        else:
            os.kill(pid, signal)

    def environ(self, var_name):
        if self.remote:
            # Get env variable
            check_command = f"echo ${var_name}"
            stdin, stdout, stderr = self.ssh.exec_command(check_command)
            stdout.channel.recv_exit_status()
            var_val = stdout.read().strip().decode('utf-8')
        else:
            var_val = os.environ.get(var_name)
        return var_val

    def execute(self, cmd):
        if self.remote:
            stdin, stdout, stderr = self.ssh.exec_command(cmd)
            stdout.channel.recv_exit_status()
            result = stdout.read().decode('utf-8')
        else:
            result = subprocess.check_output([cmd]).decode('utf-8')
        return result


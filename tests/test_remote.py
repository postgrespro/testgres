#!/usr/bin/env python
# coding: utf-8

import os
import time

import pytest
from docker import DockerClient
from paramiko import RSAKey

from testgres import RemoteOperations


class TestRemoteOperations:
    @pytest.fixture(scope="class", autouse=True)
    def setup_class(self):
        # Create shared volume
        self.volume_path = os.path.abspath("./tmp/ssh_key")
        os.makedirs(self.volume_path, exist_ok=True)

        # Generate SSH keys
        private_key_path = os.path.join(self.volume_path, "id_rsa")
        public_key_path = os.path.join(self.volume_path, "id_rsa.pub")

        private_key = RSAKey.generate(4096)
        private_key.write_private_key_file(private_key_path)

        with open(public_key_path, "w") as f:
            f.write(f"{private_key.get_name()} {private_key.get_base64()}")

        self.docker = DockerClient.from_env()
        self.container = self.docker.containers.run(
            "rastasheep/ubuntu-sshd:18.04",
            detach=True,
            tty=True,
            ports={22: 8022},
        )

        # Wait for the container to start sshd
        time.sleep(10)

        yield

        # Stop and remove the container after tests
        self.container.stop()
        self.container.remove()

    @pytest.fixture(scope="function", autouse=True)
    def setup(self):
        self.operations = RemoteOperations(
            host="localhost",
            username="root",
            ssh_key=os.path.join(self.volume_path, "id_rsa")
        )

        yield

        self.operations.__del__()

    def test_exec_command(self):
        cmd = "python3 --version"
        response = self.operations.exec_command(cmd)

        assert "Python 3.9" in response

    def test_is_executable(self):
        cmd = "python3"
        response = self.operations.is_executable(cmd)

        assert response is True

    def test_makedirs_and_rmdirs(self):
        path = "/test_dir"

        # Test makedirs
        self.operations.makedirs(path)
        assert self.operations.path_exists(path)

        # Test rmdirs
        self.operations.rmdirs(path)
        assert not self.operations.path_exists(path)

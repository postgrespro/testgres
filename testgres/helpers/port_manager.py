import socket
import random
from typing import Set, Iterable, Optional


class PortForException(Exception):
    pass


class PortManager:
    def __init__(self, ports_range=(1024, 65535)):
        self.ports_range = ports_range

    @staticmethod
    def is_port_free(port: int) -> bool:
        """Check if a port is free to use."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("", port))
                return True
            except OSError:
                return False

    def find_free_port(self, ports: Optional[Set[int]] = None, exclude_ports: Optional[Iterable[int]] = None) -> int:
        """Return a random unused port number."""
        if ports is None:
            ports = set(range(1024, 65535))

        assert type(ports) == set  # noqa: E721

        if exclude_ports is not None:
            assert isinstance(exclude_ports, Iterable)
            ports.difference_update(exclude_ports)

        sampled_ports = random.sample(tuple(ports), min(len(ports), 100))

        for port in sampled_ports:
            if self.is_port_free(port):
                return port

        raise PortForException("Can't select a port")

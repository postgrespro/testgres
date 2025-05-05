class PortManager:
    def __init__(self):
        super().__init__()

    def reserve_port(self) -> int:
        raise NotImplementedError("PortManager::reserve_port is not implemented.")

    def release_port(self, number: int) -> None:
        assert type(number) == int  # noqa: E721
        raise NotImplementedError("PortManager::release_port is not implemented.")

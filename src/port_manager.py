from .raise_error import RaiseError


class PortManager:
    def __init__(self):
        super().__init__()

    def reserve_port(self) -> int:
        RaiseError.method_is_not_implemented(__class__, "reserve_port")

    def release_port(self, number: int) -> None:
        assert type(number) is int
        RaiseError.method_is_not_implemented(__class__, "release_port")

class Helpers:
    def PrepareProcessInput(input, encoding):
        if not input:
            return None

        if type(input) == str:  # noqa: E721
            if encoding is None:
                return input.encode()

            assert type(encoding) == str  # noqa: E721
            return input.encode(encoding)

        # It is expected!
        assert type(input) == bytes  # noqa: E721
        return input

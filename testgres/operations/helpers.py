import locale


class Helpers:
    def _make_get_default_encoding_func():
        # locale.getencoding is added in Python 3.11
        if hasattr(locale, 'getencoding'):
            return locale.getencoding

        # It must exist
        return locale.getpreferredencoding

    # Prepared pointer on function to get a name of system codepage
    _get_default_encoding_func = _make_get_default_encoding_func()

    def GetDefaultEncoding():
        #
        #   Original idea/source was:
        #
        #   def os_ops.get_default_encoding():
        #       if not hasattr(locale, 'getencoding'):
        #       locale.getencoding = locale.getpreferredencoding
        #       return locale.getencoding() or 'UTF-8'
        #

        assert __class__._get_default_encoding_func is not None

        r = __class__._get_default_encoding_func()

        if r:
            assert r is not None
            assert type(r) == str  # noqa: E721
            assert r != ""
            return r

        # Is it an unexpected situation?
        return 'UTF-8'

    def PrepareProcessInput(input, encoding):
        if not input:
            return None

        if type(input) == str:  # noqa: E721
            if encoding is None:
                return input.encode(__class__.GetDefaultEncoding())

            assert type(encoding) == str  # noqa: E721
            return input.encode(encoding)

        # It is expected!
        assert type(input) == bytes  # noqa: E721
        return input

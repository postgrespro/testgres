import six
import functools


def positional_args_hack(*special_cases):
    """
    Convert positional args described by
    'special_cases' into named args.

    Example:
        @positional_args_hack(['abc'], ['def', 'abc'])
        def some_api_func(...)

    This is useful for compatibility.
    """

    cases = dict()

    for case in special_cases:
        k = len(case)
        assert k not in six.iterkeys(cases), 'len must be unique'
        cases[k] = case

    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            k = len(args)

            if k in six.iterkeys(cases):
                case = cases[k]

                for i in range(0, k):
                    arg_name = case[i]
                    arg_val = args[i]

                    # transform into named
                    kwargs[arg_name] = arg_val

                # get rid of them
                args = []

            return function(*args, **kwargs)

        return wrapper

    return decorator


def method_decorator(decorator):
    """
    Convert a function decorator into a method decorator.
    """
    def _dec(func):
        def _wrapper(self, *args, **kwargs):
            @decorator
            def bound_func(*args2, **kwargs2):
                return func.__get__(self, type(self))(*args2, **kwargs2)

            # 'bound_func' is a closure and can see 'self'
            return bound_func(*args, **kwargs)

        # preserve docs
        functools.update_wrapper(_wrapper, func)

        return _wrapper

    # preserve docs
    functools.update_wrapper(_dec, decorator)

    # change name for easier debugging
    _dec.__name__ = 'method_decorator({})'.format(decorator.__name__)

    return _dec

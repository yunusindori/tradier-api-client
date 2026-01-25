"""
Common decorators
"""
import functools


def is_authenticated():
    """
    Checks whether the api_key is set on the object
    """

    def decorator(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            if not getattr(self, "authenticated", None):
                raise Exception("API key is not set, API call will fail")
            return method(self, *args, **kwargs)

        return wrapper

    return decorator

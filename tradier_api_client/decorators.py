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
            auth_state = getattr(self, "is_authenticated", None)
            if auth_state is None:
                auth_state = getattr(self, "authenticated", None)
                if callable(auth_state):
                    auth_state = auth_state()
            if not auth_state:
                raise Exception("API key is not set, API call will fail")
            return method(self, *args, **kwargs)

        return wrapper

    return decorator

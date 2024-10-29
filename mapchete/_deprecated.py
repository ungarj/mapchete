import warnings
from functools import wraps
from typing import Callable, TypeVar, Any, cast, Type, Union

# Define a generic type for functions and classes
F = TypeVar("F", bound=Callable[..., Any])
C = TypeVar("C", bound=Type[Any])


def deprecated(reason: str = "") -> Callable[[Union[F, C]], Union[F, C]]:
    """Decorator to mark functions or classes as deprecated.

    Args:
        reason (str): Optional reason or guidance for deprecation.
    Returns:
        Callable[[Union[F, C]], Union[F, C]]: The decorated function or class that issues a DeprecationWarning.
    """

    def decorator(obj: Union[F, C]) -> Union[F, C]:
        message = f"{obj.__name__} is deprecated."
        if reason:
            message += f" {reason}"

        if isinstance(obj, type):  # Check if obj is a class
            # Decorate a class to show warning on instantiation
            original_init = obj.__init__

            @wraps(original_init)
            def new_init(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
                warnings.warn(message, category=DeprecationWarning, stacklevel=2)
                original_init(self, *args, **kwargs)

            obj.__init__ = new_init  # Replace the original __init__ with new_init
            obj.__doc__ = f"DEPRECATED: {reason}\n\n{obj.__doc__}"  # Update docstring
            return cast(C, obj)

        else:
            # Decorate a function to show warning on call
            @wraps(obj)
            def wrapper(*args: Any, **kwargs: Any) -> Any:  # pragma: no cover
                warnings.warn(message, category=DeprecationWarning, stacklevel=2)
                return obj(*args, **kwargs)

            wrapper.__doc__ = f"DEPRECATED: {reason}\n\n{obj.__doc__}"
            return cast(F, wrapper)

    return decorator

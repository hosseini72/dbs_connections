"""
Database connection management library with support for multiple DBMSs.
"""

# Import core functionality
from .connectors import *  # noqa: F401
from .core import *  # noqa: F401, F403

# Import middleware when available
try:
    from .middleware.fastapi import FastAPIMiddleware

    _has_fastapi_middleware = True
except ImportError:
    _has_fastapi_middleware = False

try:
    from .middleware.django import DjangoMiddleware

    _has_django_middleware = True
except ImportError:
    _has_django_middleware = False

# Build __all__ dynamically
__all__ = []

# Add core exports
from .core import __all__ as core_all  # noqa: F401

__all__.extend(core_all)

# Add connector exports
from .connectors import __all__ as connectors_all  # noqa: F401

__all__.extend(connectors_all)

# Add middleware if available
if _has_fastapi_middleware:
    __all__.append("FastAPIMiddleware")
if _has_django_middleware:
    __all__.append("DjangoMiddleware")

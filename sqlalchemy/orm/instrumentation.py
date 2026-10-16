# Fix: typing.Generic instrumentation conflict detection
from typing import Generic, TypeVar

T = TypeVar('T')

class InstrumentationConflictDetector:
    def check_conflict(self, cls1, cls2):
        """
        Check if two classes have instrumentation conflicts.
        typing.Generic subclasses should not be treated as related.
        """
        # Fix: Skip conflict detection for typing.Generic classes
        if hasattr(cls1, '__mro__') and Generic in cls1.__mro__:
            return False
        if hasattr(cls2, '__mro__') and Generic in cls2.__mro__:
            return False
        
        # Standard conflict detection for non-Generic classes
        return self._detect_conflict(cls1, cls2)

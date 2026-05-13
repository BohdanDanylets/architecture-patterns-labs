import logging
import sys
 
 
def setup_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    """
    Create and configure a named logger with a consistent format.
 
    Args:
        name:  Module/stage name used as the logger identifier.
        level: Logging verbosity (default DEBUG shows all messages).
 
    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
 
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
 
        formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03d | %(name)-20s | %(levelname)-8s | %(message)s",
            datefmt="%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
 
    # Prevent log records from propagating to the root logger
    # (avoids duplicate output when root logger is also configured).
    logger.propagate = False
 
    return logger
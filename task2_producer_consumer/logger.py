import logging
import sys
 
 
def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Return a named, stream-based logger.
 
    Args:
        name:  Module / component name embedded in every log line.
        level: Verbosity threshold (default INFO for experiment runner;
               set to DEBUG for detailed per-packet tracing).
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
 
    logger.propagate = False
    return logger
 
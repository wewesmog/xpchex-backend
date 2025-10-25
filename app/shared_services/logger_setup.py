import logging
import sys
import os
from datetime import datetime

def truncate_for_logging(obj, max_length=500):
    """Truncate long strings/objects for logging.
    
    Args:
        obj: Object to truncate
        max_length: Maximum length before truncation
        
    Returns:
        Truncated string with length indicator if truncated
    """
    string_repr = str(obj)
    if len(string_repr) <= max_length:
        return string_repr
    return f"{string_repr[:max_length]}... [truncated, total length: {len(string_repr)}]"

def setup_logger(name: str = "QueryStateLogger") -> logging.Logger:
    """
    Set up a logger with proper Unicode handling.
    
    Args:
        name: Name of the logger
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Console handler with UTF-8 encoding
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        # File handler with UTF-8 encoding
        logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        log_file = os.path.join(logs_dir, f'query_state_log_{datetime.now().strftime("%Y-%m-%d")}.log')
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        
        # Add handlers
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    
    return logger
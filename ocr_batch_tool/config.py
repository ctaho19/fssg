"""
Configuration file for the OCR Batch Tool.
Contains default settings and constants.
"""

# Application information
APP_NAME = "Batch OCR Text Extractor"
APP_VERSION = "1.0.0"
APP_DESCRIPTION = "Extract text from multiple images using Tesseract OCR"

# Default OCR settings
DEFAULT_LANGUAGE = "eng"
DEFAULT_DPI = None  # Auto-detect
DEFAULT_PREPROCESSING = True
DEFAULT_MAX_WORKERS = 2
DEFAULT_COMBINE_OUTPUT = False

# Supported file formats
SUPPORTED_IMAGE_FORMATS = {
    '.jpg', '.jpeg', '.png', '.tiff', '.tif', '.bmp', '.pdf'
}

# GUI settings
DEFAULT_WINDOW_SIZE = "800x700"
MIN_WINDOW_SIZE = (600, 500)
GUI_THEME = "clam"

# Processing settings
MAX_WORKERS_LIMIT = 8
MIN_WORKERS = 1

# File patterns for directory scanning
IMAGE_PATTERNS = [
    "*.jpg", "*.jpeg", "*.png", "*.tiff", "*.tif", "*.bmp", "*.pdf",
    "*.JPG", "*.JPEG", "*.PNG", "*.TIFF", "*.TIF", "*.BMP", "*.PDF"
]

# OCR configuration
TESSERACT_CONFIG = {
    'oem': 3,  # OCR Engine Mode
    'psm': 6,  # Page Segmentation Mode
}

# Common language combinations
COMMON_LANGUAGES = {
    'English': 'eng',
    'French': 'fra',
    'German': 'deu',
    'Spanish': 'spa',
    'Chinese (Simplified)': 'chi_sim',
    'Japanese': 'jpn',
    'English + French': 'eng+fra',
    'English + German': 'eng+deu',
    'English + Spanish': 'eng+spa',
    'English + German + French': 'eng+deu+fra',
}

# DPI options
DPI_OPTIONS = ['auto', '150', '300', '600']

# Logging configuration
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
# Batch OCR Text Extractor

A powerful and user-friendly tool for extracting text from multiple images using Tesseract OCR. Supports both GUI and command-line interfaces for flexible usage.

## Features

- **Batch Processing**: Process 20+ images at once (no upper limit)
- **Multiple Formats**: Supports JPEG, PNG, TIFF, BMP, and PDF files
- **Dual Interface**: Both GUI and CLI options
- **Parallel Processing**: Multi-threaded processing for faster results
- **Image Preprocessing**: Automatic image enhancement for better OCR accuracy
- **Flexible Output**: Individual text files or combined output
- **Progress Tracking**: Real-time progress feedback
- **Error Handling**: Graceful handling of corrupted or unreadable files
- **Multi-language Support**: Supports multiple Tesseract languages

## Installation

### Prerequisites

1. **Python 3.9 or higher**
2. **Tesseract OCR Engine**

### Installing Tesseract OCR

#### Windows
1. Download the installer from: https://github.com/UB-Mannheim/tesseract/wiki
2. Run the installer and follow the setup wizard
3. Add Tesseract to your PATH environment variable
4. Verify installation: `tesseract --version`

#### macOS
```bash
# Using Homebrew
brew install tesseract

# Using MacPorts
sudo port install tesseract3
```

#### Linux (Ubuntu/Debian)
```bash
sudo apt update
sudo apt install tesseract-ocr
sudo apt install tesseract-ocr-eng  # English language pack

# For additional languages
sudo apt install tesseract-ocr-fra  # French
sudo apt install tesseract-ocr-deu  # German
sudo apt install tesseract-ocr-spa  # Spanish
```

#### Linux (CentOS/RHEL/Fedora)
```bash
# CentOS/RHEL
sudo yum install epel-release
sudo yum install tesseract

# Fedora
sudo dnf install tesseract
```

### Setting up the OCR Tool

1. **Clone or download this repository:**
```bash
git clone <repository-url>
cd ocr_batch_tool
```

2. **Create a virtual environment (recommended):**
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate
```

3. **Install Python dependencies:**
```bash
pip install -r requirements.txt
```

4. **Verify installation:**
```bash
python batch_ocr.py --version
```

## Usage

### GUI Mode (Default)

Launch the graphical interface:
```bash
python batch_ocr.py
```

#### GUI Features:
- **File Selection**: Click "Select Images" or drag and drop files
- **Configuration**: Set language, DPI, and processing options
- **Output Settings**: Choose output directory and format
- **Progress Monitoring**: Real-time progress bar and detailed logs
- **Error Reporting**: Clear error messages and file-specific issues

### Command Line Interface

Use the CLI for automation and scripting:

#### Basic Usage
```bash
# Process images from a directory
python batch_ocr.py --cli --input /path/to/images --output /path/to/output

# Process specific files
python batch_ocr.py --cli --input file1.jpg file2.png --output ./results
```

#### Advanced Options
```bash
# Enable parallel processing with 4 workers
python batch_ocr.py --cli --input /path/to/images --output ./results --parallel --workers 4

# Use multiple languages and combine output
python batch_ocr.py --cli --input /path/to/images --output ./results --language eng+fra --combine

# Custom DPI and disable preprocessing
python batch_ocr.py --cli --input /path/to/images --output ./results --dpi 300 --no-preprocessing

# Verbose output for debugging
python batch_ocr.py --cli --input /path/to/images --output ./results --verbose
```

#### CLI Options Reference

| Option | Short | Description |
|--------|-------|-------------|
| `--input` | `-i` | Input files or directories (required) |
| `--output` | `-o` | Output directory (required) |
| `--language` | `-l` | Tesseract language code (default: eng) |
| `--dpi` | | DPI setting for OCR (default: auto) |
| `--no-preprocessing` | | Disable image preprocessing |
| `--combine` | | Combine all text into single file |
| `--parallel` | | Enable parallel processing |
| `--workers` | | Number of worker threads (default: 2) |
| `--verbose` | `-v` | Enable verbose logging |
| `--quiet` | `-q` | Suppress progress output |
| `--version` | | Show version information |

### Language Codes

Common Tesseract language codes:
- `eng` - English
- `fra` - French
- `deu` - German
- `spa` - Spanish
- `chi_sim` - Chinese Simplified
- `jpn` - Japanese
- `eng+fra` - English and French
- `eng+deu+fra` - English, German, and French

## Examples

### GUI Examples

1. **Basic batch processing:**
   - Launch GUI: `python batch_ocr.py`
   - Click "Select Images" and choose your files
   - Click "Browse" to select output directory
   - Click "Start OCR Processing"

2. **Advanced configuration:**
   - Set language to "eng+fra" for bilingual documents
   - Enable parallel processing for faster results
   - Check "Combine all text into single file" if desired

### CLI Examples

1. **Process a directory of images:**
```bash
python batch_ocr.py --cli --input ~/Documents/scans --output ~/Documents/extracted_text
```

2. **High-performance processing:**
```bash
python batch_ocr.py --cli \
    --input ~/Documents/scans \
    --output ~/Documents/extracted_text \
    --parallel --workers 8 \
    --dpi 300
```

3. **Multilingual document processing:**
```bash
python batch_ocr.py --cli \
    --input ~/Documents/multilingual \
    --output ~/Documents/text_output \
    --language eng+fra+deu \
    --combine
```

## Troubleshooting

### Common Issues

1. **"Tesseract not found" error:**
   - Ensure Tesseract is installed and in your PATH
   - On Windows, check that the Tesseract installation directory is in your PATH
   - Test with: `tesseract --version`

2. **Import errors:**
   - Ensure all dependencies are installed: `pip install -r requirements.txt`
   - Check Python version: `python --version` (should be 3.9+)

3. **Poor OCR accuracy:**
   - Try increasing DPI: `--dpi 300` or `--dpi 600`
   - Ensure images are clear and high-contrast
   - Enable preprocessing (default, or ensure `--no-preprocessing` is not used)
   - Use appropriate language codes

4. **GUI won't start:**
   - Ensure tkinter is available: `python -c "import tkinter"`
   - Use CLI mode as alternative: `python batch_ocr.py --cli`

5. **Memory issues with large batches:**
   - Reduce number of parallel workers: `--workers 2`
   - Process in smaller batches
   - Ensure sufficient system RAM

### Performance Tips

1. **For large batches (100+ images):**
   - Use parallel processing: `--parallel --workers 4`
   - Use CLI mode for better performance
   - Process in chunks if memory is limited

2. **For better accuracy:**
   - Use higher DPI settings for small text
   - Ensure good image quality (contrast, resolution)
   - Use appropriate language settings

3. **For faster processing:**
   - Disable preprocessing if images are already clean
   - Use fewer workers on slower systems
   - Use SSD storage for input/output

## Technical Details

### Supported File Formats
- **Images**: JPEG (.jpg, .jpeg), PNG (.png), TIFF (.tiff, .tif), BMP (.bmp)
- **Documents**: PDF (.pdf) - text extraction with OCR fallback

### Image Preprocessing
The tool automatically applies the following preprocessing steps:
1. Grayscale conversion
2. Gaussian blur for noise reduction
3. Adaptive thresholding
4. Contrast enhancement

### Architecture
- **Core Engine**: `ocr_processor.py` - Main OCR processing logic
- **GUI Interface**: `gui_app.py` - Tkinter-based graphical interface
- **CLI Interface**: `cli_app.py` - Command-line argument processing
- **Main Entry**: `batch_ocr.py` - Application launcher

## Development

### Project Structure
```
ocr_batch_tool/
├── batch_ocr.py          # Main entry point
├── requirements.txt      # Python dependencies
├── README.md            # This file
├── src/
│   ├── ocr_processor.py # Core OCR processing
│   ├── gui_app.py       # GUI interface
│   └── cli_app.py       # CLI interface
├── tests/               # Test files
└── sample_images/       # Sample images for testing
```

### Contributing
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

### Testing
```bash
# Run with sample images
python batch_ocr.py --cli --input sample_images --output test_output --verbose
```

## License

This project is released under the MIT License. See LICENSE file for details.

## Acknowledgments

- [Tesseract OCR](https://github.com/tesseract-ocr/tesseract) - OCR engine
- [pytesseract](https://github.com/madmaze/pytesseract) - Python wrapper
- [Pillow](https://pillow.readthedocs.io/) - Image processing
- [OpenCV](https://opencv.org/) - Computer vision operations

---

For support, feature requests, or bug reports, please open an issue on the project repository.
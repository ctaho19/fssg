"""
Command Line Interface for batch OCR processing.
Provides a simple command-line interface for batch image text extraction.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List
import glob
import logging

from ocr_processor import OCRProcessor


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def find_image_files(input_paths: List[str]) -> List[str]:
    """
    Find all supported image files from input paths.
    
    Args:
        input_paths: List of file paths or directory paths
        
    Returns:
        List of image file paths
    """
    image_files = []
    supported_extensions = {'.jpg', '.jpeg', '.png', '.tiff', '.tif', '.bmp', '.pdf'}
    
    for input_path in input_paths:
        path = Path(input_path)
        
        if path.is_file():
            if path.suffix.lower() in supported_extensions:
                image_files.append(str(path))
            else:
                logging.warning(f"Skipping unsupported file: {input_path}")
        
        elif path.is_dir():
            # Search for image files in directory
            for ext in supported_extensions:
                pattern = str(path / f"*{ext}")
                image_files.extend(glob.glob(pattern))
                # Also search for uppercase extensions
                pattern = str(path / f"*{ext.upper()}")
                image_files.extend(glob.glob(pattern))
        
        else:
            logging.warning(f"Path not found: {input_path}")
    
    # Remove duplicates and sort
    image_files = sorted(list(set(image_files)))
    return image_files


def progress_callback(current: int, total: int, current_file: str):
    """Progress callback function for CLI interface."""
    filename = Path(current_file).name
    progress_percent = (current / total) * 100
    print(f"Processing [{current}/{total}] ({progress_percent:.1f}%): {filename}")


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Batch OCR Text Extractor - Extract text from multiple images using Tesseract OCR",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --input /path/to/images --output /path/to/output
  %(prog)s --input file1.jpg file2.png --output ./results
  %(prog)s --input /path/to/images --output ./results --combine
  %(prog)s --input /path/to/images --output ./results --language eng+fra --parallel --workers 4
        """
    )
    
    # Input arguments
    parser.add_argument(
        '--input', '-i',
        nargs='+',
        required=True,
        help='Input image files or directories containing images'
    )
    
    parser.add_argument(
        '--output', '-o',
        required=True,
        help='Output directory for extracted text files'
    )
    
    # OCR configuration
    parser.add_argument(
        '--language', '-l',
        default='eng',
        help='Tesseract language code (default: eng). Examples: eng, eng+fra, chi_sim'
    )
    
    parser.add_argument(
        '--dpi',
        type=int,
        help='DPI setting for Tesseract (default: auto-detect)'
    )
    
    parser.add_argument(
        '--no-preprocessing',
        action='store_true',
        help='Disable image preprocessing (may reduce accuracy)'
    )
    
    # Output configuration
    parser.add_argument(
        '--combine',
        action='store_true',
        help='Combine all extracted text into a single file'
    )
    
    # Processing configuration
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Enable parallel processing'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=2,
        help='Number of worker threads for parallel processing (default: 2)'
    )
    
    # Utility arguments
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress progress output'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='Batch OCR Text Extractor 1.0.0'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Validate arguments
    if args.parallel and args.workers < 1:
        parser.error("Number of workers must be at least 1")
    
    if args.verbose and args.quiet:
        parser.error("Cannot use both --verbose and --quiet")
    
    # Find image files
    logging.info("Searching for image files...")
    image_files = find_image_files(args.input)
    
    if not image_files:
        logging.error("No supported image files found in input paths")
        sys.exit(1)
    
    logging.info(f"Found {len(image_files)} image files")
    
    # Create output directory
    output_dir = Path(args.output)
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Output directory: {output_dir.absolute()}")
    except Exception as e:
        logging.error(f"Failed to create output directory: {e}")
        sys.exit(1)
    
    # Initialize OCR processor
    logging.info("Initializing OCR processor...")
    try:
        max_workers = args.workers if args.parallel else 1
        
        processor = OCRProcessor(
            language=args.language,
            enable_preprocessing=not args.no_preprocessing,
            dpi=args.dpi,
            max_workers=max_workers
        )
        
        logging.info(f"OCR Configuration:")
        logging.info(f"  Language: {args.language}")
        logging.info(f"  DPI: {args.dpi or 'auto'}")
        logging.info(f"  Preprocessing: {not args.no_preprocessing}")
        logging.info(f"  Parallel processing: {args.parallel}")
        if args.parallel:
            logging.info(f"  Workers: {args.workers}")
        logging.info(f"  Combined output: {args.combine}")
        
    except Exception as e:
        logging.error(f"Failed to initialize OCR processor: {e}")
        sys.exit(1)
    
    # Process images
    logging.info("Starting OCR processing...")
    try:
        # Setup progress callback
        callback = None if args.quiet else progress_callback
        
        results = processor.process_batch(
            image_paths=image_files,
            output_dir=str(output_dir),
            combine_output=args.combine,
            progress_callback=callback
        )
        
        # Report results
        success_count = sum(1 for result in results.values() if result == "Success")
        total_count = len(results)
        
        print(f"\n=== Processing Complete ===")
        print(f"Successfully processed: {success_count}/{total_count} files")
        
        # Report errors
        errors = {path: error for path, error in results.items() if error != "Success"}
        if errors:
            print(f"\nErrors encountered:")
            for path, error in errors.items():
                filename = Path(path).name
                print(f"  {filename}: {error}")
        
        # Output file information
        if args.combine:
            combined_file = output_dir / "combined_extracted_text.txt"
            if combined_file.exists():
                print(f"\nCombined text saved to: {combined_file}")
        else:
            print(f"\nIndividual text files saved to: {output_dir}")
        
        # Exit with appropriate code
        if success_count == total_count:
            logging.info("All files processed successfully")
            sys.exit(0)
        else:
            logging.warning(f"{total_count - success_count} files failed to process")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logging.info("Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Processing failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
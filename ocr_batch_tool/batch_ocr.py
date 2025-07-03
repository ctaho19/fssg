#!/usr/bin/env python3
"""
Batch OCR Text Extractor - Main Entry Point
Provides both GUI and CLI interfaces for batch image OCR processing.
"""

import sys
import os
import argparse
from pathlib import Path

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def main():
    """Main entry point that determines whether to run GUI or CLI."""
    
    # Create a simple argument parser for mode selection
    parser = argparse.ArgumentParser(
        description="Batch OCR Text Extractor",
        add_help=False  # We'll handle help ourselves
    )
    
    parser.add_argument(
        '--gui',
        action='store_true',
        help='Launch GUI interface (default if no arguments)'
    )
    
    parser.add_argument(
        '--cli',
        action='store_true',
        help='Use command-line interface'
    )
    
    # Parse known args to check for mode selection
    known_args, remaining_args = parser.parse_known_args()
    
    # Determine mode
    if known_args.cli or remaining_args:
        # CLI mode - either explicitly requested or has CLI arguments
        from cli_app import main as cli_main
        
        # Restore original argv for CLI parser
        sys.argv = [sys.argv[0]] + remaining_args
        cli_main()
    
    else:
        # GUI mode - default when no arguments or --gui specified
        try:
            from gui_app import main as gui_main
            gui_main()
        except ImportError as e:
            print(f"Error: Could not import GUI dependencies: {e}")
            print("Please ensure tkinter is available, or use --cli mode")
            sys.exit(1)
        except Exception as e:
            print(f"Error launching GUI: {e}")
            print("Try using --cli mode instead")
            sys.exit(1)


if __name__ == "__main__":
    main()
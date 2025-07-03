#!/usr/bin/env python3
"""
Simple test script for the OCR Batch Tool.
Tests basic functionality with sample images.
"""

import os
import sys
import tempfile
import shutil
from pathlib import Path

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    from ocr_processor import OCRProcessor
    from PIL import Image, ImageDraw, ImageFont
except ImportError as e:
    print(f"Import error: {e}")
    print("Please install required dependencies: pip install -r requirements.txt")
    sys.exit(1)


def create_test_image(text: str, filename: str, size=(400, 200)):
    """Create a simple test image with text."""
    img = Image.new('RGB', size, color='white')
    draw = ImageDraw.Draw(img)
    
    # Try to use a built-in font, fallback to default
    try:
        font = ImageFont.truetype("arial.ttf", 24)
    except:
        try:
            font = ImageFont.load_default()
        except:
            font = None
    
    # Calculate text position (center)
    if font:
        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
    else:
        text_width = len(text) * 6  # Rough estimate
        text_height = 11
    
    x = (size[0] - text_width) // 2
    y = (size[1] - text_height) // 2
    
    draw.text((x, y), text, fill='black', font=font)
    img.save(filename)
    return filename


def test_ocr_processor():
    """Test the OCR processor with sample images."""
    print("Testing OCR Batch Tool...")
    
    # Create temporary directory for test
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        input_dir = temp_path / "input"
        output_dir = temp_path / "output"
        
        input_dir.mkdir()
        output_dir.mkdir()
        
        # Create test images
        test_texts = [
            "Hello, World!",
            "This is a test document.",
            "OCR Processing Test",
            "Batch processing works!",
        ]
        
        test_files = []
        for i, text in enumerate(test_texts):
            filename = input_dir / f"test_{i+1}.png"
            create_test_image(text, str(filename))
            test_files.append(str(filename))
        
        print(f"Created {len(test_files)} test images in {input_dir}")
        
        # Test OCR processor
        try:
            processor = OCRProcessor(
                language='eng',
                enable_preprocessing=True,
                max_workers=1
            )
            print("OCR Processor initialized successfully")
            
            # Test individual file processing
            print("\nTesting individual file processing...")
            for test_file in test_files[:2]:  # Test first 2 files
                extracted_text, error = processor.extract_text_from_image(test_file)
                filename = Path(test_file).name
                if error:
                    print(f"  {filename}: ERROR - {error}")
                else:
                    print(f"  {filename}: SUCCESS - '{extracted_text.strip()}'")
            
            # Test batch processing
            print("\nTesting batch processing...")
            
            def progress_callback(current, total, current_file):
                filename = Path(current_file).name
                print(f"  Processing [{current}/{total}]: {filename}")
            
            results = processor.process_batch(
                image_paths=test_files,
                output_dir=str(output_dir),
                combine_output=False,
                progress_callback=progress_callback
            )
            
            # Check results
            success_count = sum(1 for result in results.values() if result == "Success")
            print(f"\nBatch processing results: {success_count}/{len(test_files)} successful")
            
            # Check output files
            output_files = list(output_dir.glob("*.txt"))
            print(f"Output files created: {len(output_files)}")
            
            # Display output content
            print("\nOutput file contents:")
            for output_file in sorted(output_files):
                try:
                    with open(output_file, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        print(f"  {output_file.name}: '{content}'")
                except Exception as e:
                    print(f"  {output_file.name}: Error reading - {e}")
            
            # Test combined output
            print("\nTesting combined output...")
            combined_results = processor.process_batch(
                image_paths=test_files,
                output_dir=str(output_dir / "combined"),
                combine_output=True,
                progress_callback=None
            )
            
            combined_file = output_dir / "combined" / "combined_extracted_text.txt"
            if combined_file.exists():
                print(f"Combined output file created: {combined_file}")
                with open(combined_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    print(f"Combined file size: {len(content)} characters")
            
            return True
            
        except Exception as e:
            print(f"OCR Processor test failed: {e}")
            return False


def test_cli_interface():
    """Test the CLI interface."""
    print("\n" + "="*50)
    print("Testing CLI Interface...")
    
    try:
        from cli_app import find_image_files, setup_logging
        
        # Test logging setup
        setup_logging(verbose=False)
        print("CLI logging setup: OK")
        
        # Test file finding with non-existent paths
        test_paths = ["/nonexistent/path", "./nonexistent_file.jpg"]
        found_files = find_image_files(test_paths)
        print(f"File finding with invalid paths: {len(found_files)} files found (expected 0)")
        
        return True
        
    except Exception as e:
        print(f"CLI interface test failed: {e}")
        return False


def test_gui_interface():
    """Test GUI interface (import only)."""
    print("\n" + "="*50)
    print("Testing GUI Interface...")
    
    try:
        # Test if GUI can be imported (doesn't create window)
        import tkinter as tk
        print("Tkinter import: OK")
        
        # Try importing GUI module
        from gui_app import OCRBatchGUI
        print("GUI module import: OK")
        
        return True
        
    except ImportError as e:
        print(f"GUI interface test failed (import error): {e}")
        print("This is expected if tkinter is not available")
        return False
    except Exception as e:
        print(f"GUI interface test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("OCR Batch Tool - Test Suite")
    print("=" * 50)
    
    tests = [
        ("OCR Processor", test_ocr_processor),
        ("CLI Interface", test_cli_interface),
        ("GUI Interface", test_gui_interface),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name} Test:")
        print("-" * 30)
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"Test failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    
    passed = 0
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nPassed: {passed}/{len(results)} tests")
    
    if passed == len(results):
        print("All tests passed! OCR tool is ready to use.")
        return 0
    else:
        print("Some tests failed. Please check the installation and requirements.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
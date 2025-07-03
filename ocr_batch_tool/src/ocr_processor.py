"""
Core OCR processing module for batch image text extraction.
Handles image preprocessing, OCR operations, and output file management.
"""

import os
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

import pytesseract
from PIL import Image, ImageEnhance, ImageFilter
import cv2
import numpy as np
from PyPDF2 import PdfReader
from tqdm import tqdm

class OCRProcessor:
    """
    Main OCR processor class that handles batch image text extraction.
    """
    
    SUPPORTED_FORMATS = {'.jpg', '.jpeg', '.png', '.tiff', '.tif', '.bmp', '.pdf'}
    
    def __init__(self, 
                 language: str = 'eng',
                 enable_preprocessing: bool = True,
                 dpi: Optional[int] = None,
                 max_workers: int = 1):
        """
        Initialize OCR processor with configuration options.
        
        Args:
            language: Tesseract language code (e.g., 'eng', 'eng+fra')
            enable_preprocessing: Whether to apply image preprocessing
            dpi: DPI setting for Tesseract (None for auto)
            max_workers: Number of parallel threads for processing
        """
        self.language = language
        self.enable_preprocessing = enable_preprocessing
        self.dpi = dpi
        self.max_workers = max_workers
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        self._setup_logging()
        
        # Validate Tesseract installation
        self._validate_tesseract()
    
    def _setup_logging(self):
        """Setup logging configuration."""
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _validate_tesseract(self):
        """Validate that Tesseract is properly installed and accessible."""
        try:
            pytesseract.get_tesseract_version()
            self.logger.info("Tesseract validation successful")
        except Exception as e:
            self.logger.error(f"Tesseract validation failed: {e}")
            raise RuntimeError(
                "Tesseract not found. Please install Tesseract-OCR and ensure it's in your PATH."
            )
    
    def get_supported_files(self, file_paths: List[str]) -> List[str]:
        """
        Filter input files to only include supported formats.
        
        Args:
            file_paths: List of file paths to filter
            
        Returns:
            List of supported file paths
        """
        supported_files = []
        for file_path in file_paths:
            if Path(file_path).suffix.lower() in self.SUPPORTED_FORMATS:
                supported_files.append(file_path)
            else:
                self.logger.warning(f"Unsupported file format: {file_path}")
        
        return supported_files
    
    def preprocess_image(self, image: Image.Image) -> Image.Image:
        """
        Apply preprocessing to improve OCR accuracy.
        
        Args:
            image: PIL Image object
            
        Returns:
            Preprocessed PIL Image object
        """
        if not self.enable_preprocessing:
            return image
        
        try:
            # Convert to grayscale if not already
            if image.mode != 'L':
                image = image.convert('L')
            
            # Convert PIL to OpenCV format for advanced processing
            img_array = np.array(image)
            
            # Apply Gaussian blur to reduce noise
            img_array = cv2.GaussianBlur(img_array, (1, 1), 0)
            
            # Apply adaptive thresholding
            img_array = cv2.adaptiveThreshold(
                img_array, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                cv2.THRESH_BINARY, 11, 2
            )
            
            # Convert back to PIL
            processed_image = Image.fromarray(img_array)
            
            # Enhance contrast
            enhancer = ImageEnhance.Contrast(processed_image)
            processed_image = enhancer.enhance(1.2)
            
            return processed_image
            
        except Exception as e:
            self.logger.warning(f"Preprocessing failed, using original image: {e}")
            return image
    
    def extract_text_from_image(self, image_path: str) -> Tuple[str, str]:
        """
        Extract text from a single image file.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Tuple of (extracted_text, error_message)
        """
        try:
            file_path = Path(image_path)
            
            if file_path.suffix.lower() == '.pdf':
                return self._extract_text_from_pdf(image_path)
            else:
                return self._extract_text_from_image_file(image_path)
                
        except Exception as e:
            error_msg = f"Failed to process {image_path}: {str(e)}"
            self.logger.error(error_msg)
            return "", error_msg
    
    def _extract_text_from_image_file(self, image_path: str) -> Tuple[str, str]:
        """Extract text from standard image files."""
        try:
            # Open and preprocess image
            with Image.open(image_path) as image:
                processed_image = self.preprocess_image(image)
                
                # Configure Tesseract parameters
                custom_config = f'--oem 3 --psm 6 -l {self.language}'
                if self.dpi:
                    custom_config += f' --dpi {self.dpi}'
                
                # Extract text
                extracted_text = pytesseract.image_to_string(
                    processed_image, 
                    config=custom_config
                ).strip()
                
                return extracted_text, ""
                
        except Exception as e:
            return "", str(e)
    
    def _extract_text_from_pdf(self, pdf_path: str) -> Tuple[str, str]:
        """Extract text from PDF files."""
        try:
            extracted_text = ""
            
            # First try to extract text directly (for text-based PDFs)
            try:
                with open(pdf_path, 'rb') as file:
                    pdf_reader = PdfReader(file)
                    for page in pdf_reader.pages:
                        text = page.extract_text()
                        if text.strip():
                            extracted_text += text + "\n"
                
                if extracted_text.strip():
                    return extracted_text.strip(), ""
            except:
                pass
            
            # If no text found, treat as image-based PDF (OCR each page)
            # This would require pdf2image library for full implementation
            self.logger.warning(f"PDF {pdf_path} may be image-based. Consider using pdf2image for better support.")
            return extracted_text.strip(), ""
            
        except Exception as e:
            return "", str(e)
    
    def process_batch(self, 
                     image_paths: List[str],
                     output_dir: str,
                     combine_output: bool = False,
                     progress_callback: Optional[Callable] = None) -> Dict[str, str]:
        """
        Process a batch of images and save extracted text.
        
        Args:
            image_paths: List of image file paths
            output_dir: Directory to save extracted text files
            combine_output: Whether to combine all text into one file
            progress_callback: Optional callback function for progress updates
            
        Returns:
            Dictionary mapping file paths to results/errors
        """
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Filter supported files
        supported_files = self.get_supported_files(image_paths)
        self.logger.info(f"Processing {len(supported_files)} supported files")
        
        results = {}
        all_extracted_text = []
        
        if self.max_workers == 1:
            # Sequential processing
            for i, image_path in enumerate(supported_files):
                if progress_callback:
                    progress_callback(i, len(supported_files), image_path)
                
                extracted_text, error = self.extract_text_from_image(image_path)
                results[image_path] = error if error else "Success"
                
                if not error:
                    if combine_output:
                        all_extracted_text.append((image_path, extracted_text))
                    else:
                        self._save_individual_text(image_path, extracted_text, output_dir)
        else:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_path = {
                    executor.submit(self.extract_text_from_image, path): path 
                    for path in supported_files
                }
                
                completed = 0
                for future in as_completed(future_to_path):
                    image_path = future_to_path[future]
                    completed += 1
                    
                    if progress_callback:
                        progress_callback(completed, len(supported_files), image_path)
                    
                    try:
                        extracted_text, error = future.result()
                        results[image_path] = error if error else "Success"
                        
                        if not error:
                            if combine_output:
                                all_extracted_text.append((image_path, extracted_text))
                            else:
                                self._save_individual_text(image_path, extracted_text, output_dir)
                    except Exception as e:
                        results[image_path] = str(e)
        
        # Save combined output if requested
        if combine_output and all_extracted_text:
            self._save_combined_text(all_extracted_text, output_dir)
        
        # Log summary
        success_count = sum(1 for result in results.values() if result == "Success")
        self.logger.info(f"Processing complete: {success_count}/{len(supported_files)} files successful")
        
        return results
    
    def _save_individual_text(self, image_path: str, text: str, output_dir: str):
        """Save extracted text to individual file."""
        image_name = Path(image_path).stem
        output_file = Path(output_dir) / f"{image_name}.txt"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(text)
        except Exception as e:
            self.logger.error(f"Failed to save text for {image_path}: {e}")
    
    def _save_combined_text(self, all_text: List[Tuple[str, str]], output_dir: str):
        """Save all extracted text to a single combined file."""
        output_file = Path(output_dir) / "combined_extracted_text.txt"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                for image_path, text in all_text:
                    f.write(f"=== {Path(image_path).name} ===\n")
                    f.write(text)
                    f.write("\n\n")
        except Exception as e:
            self.logger.error(f"Failed to save combined text: {e}")
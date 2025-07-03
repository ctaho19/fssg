"""
GUI application for batch OCR processing using Tkinter.
Provides an intuitive interface for selecting images and configuring OCR settings.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
from pathlib import Path
import threading
import queue
import os
from typing import List

from ocr_processor import OCRProcessor


class OCRBatchGUI:
    """
    Main GUI application for batch OCR processing.
    """
    
    def __init__(self, root):
        self.root = root
        self.root.title("Batch OCR Text Extractor")
        self.root.geometry("800x700")
        self.root.minsize(600, 500)
        
        # Configure style
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        # Initialize variables
        self.selected_files = []
        self.output_directory = tk.StringVar()
        self.language = tk.StringVar(value='eng')
        self.enable_preprocessing = tk.BooleanVar(value=True)
        self.combine_output = tk.BooleanVar(value=False)
        self.parallel_processing = tk.BooleanVar(value=False)
        self.max_workers = tk.IntVar(value=2)
        self.dpi_setting = tk.StringVar(value='auto')
        
        # Processing state
        self.is_processing = False
        self.progress_queue = queue.Queue()
        
        # Create GUI elements
        self.create_widgets()
        self.setup_drag_drop()
        
        # Start progress monitoring
        self.root.after(100, self.check_progress_queue)
    
    def create_widgets(self):
        """Create and layout all GUI widgets."""
        # Main container with padding
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky="nsew")
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # Title
        title_label = ttk.Label(main_frame, text="Batch OCR Text Extractor", 
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))
        
        # File selection section
        self.create_file_selection_section(main_frame, row=1)
        
        # Configuration section
        self.create_configuration_section(main_frame, row=2)
        
        # Output section
        self.create_output_section(main_frame, row=3)
        
        # Processing section
        self.create_processing_section(main_frame, row=4)
        
        # Progress and results section
        self.create_progress_section(main_frame, row=5)
    
    def create_file_selection_section(self, parent, row):
        """Create file selection widgets."""
        # File selection frame
        file_frame = ttk.LabelFrame(parent, text="Image Selection", padding="10")
        file_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(0, 10))
        file_frame.columnconfigure(1, weight=1)
        
        # Select files button
        select_btn = ttk.Button(file_frame, text="Select Images", 
                               command=self.select_files)
        select_btn.grid(row=0, column=0, padx=(0, 10))
        
        # File count label
        self.file_count_label = ttk.Label(file_frame, text="No files selected")
        self.file_count_label.grid(row=0, column=1, sticky=tk.W)
        
        # Clear selection button
        clear_btn = ttk.Button(file_frame, text="Clear", command=self.clear_files)
        clear_btn.grid(row=0, column=2)
        
        # File list (scrollable)
        list_frame = ttk.Frame(file_frame)
        list_frame.grid(row=1, column=0, columnspan=3, sticky="nsew", pady=(10, 0))
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)
        
        self.file_listbox = tk.Listbox(list_frame, height=8, selectmode=tk.EXTENDED)
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.file_listbox.yview)
        self.file_listbox.configure(yscrollcommand=scrollbar.set)
        
        self.file_listbox.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        
        # Drag and drop hint
        hint_label = ttk.Label(file_frame, text="💡 Tip: You can drag and drop image files here", 
                              foreground='gray')
        hint_label.grid(row=2, column=0, columnspan=3, pady=(5, 0))
    
    def create_configuration_section(self, parent, row):
        """Create configuration widgets."""
        config_frame = ttk.LabelFrame(parent, text="OCR Configuration", padding="10")
        config_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(0, 10))
        config_frame.columnconfigure(1, weight=1)
        
        # Language setting
        ttk.Label(config_frame, text="Language:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        lang_combo = ttk.Combobox(config_frame, textvariable=self.language, width=15)
        lang_combo['values'] = ('eng', 'eng+fra', 'eng+deu', 'eng+spa', 'fra', 'deu', 'spa', 'chi_sim', 'jpn')
        lang_combo.grid(row=0, column=1, sticky=tk.W)
        
        # DPI setting
        ttk.Label(config_frame, text="DPI:").grid(row=0, column=2, sticky=tk.W, padx=(20, 10))
        dpi_combo = ttk.Combobox(config_frame, textvariable=self.dpi_setting, width=10)
        dpi_combo['values'] = ('auto', '150', '300', '600')
        dpi_combo.grid(row=0, column=3, sticky=tk.W)
        
        # Preprocessing checkbox
        preprocess_cb = ttk.Checkbutton(config_frame, text="Enable image preprocessing", 
                                       variable=self.enable_preprocessing)
        preprocess_cb.grid(row=1, column=0, columnspan=2, sticky=tk.W, pady=(10, 0))
        
        # Parallel processing checkbox
        parallel_cb = ttk.Checkbutton(config_frame, text="Enable parallel processing", 
                                     variable=self.parallel_processing,
                                     command=self.toggle_parallel_options)
        parallel_cb.grid(row=1, column=2, columnspan=2, sticky=tk.W, pady=(10, 0))
        
        # Max workers (initially disabled)
        self.workers_label = ttk.Label(config_frame, text="Max workers:", state='disabled')
        self.workers_label.grid(row=2, column=0, sticky=tk.W, padx=(0, 10), pady=(5, 0))
        
        self.workers_spin = ttk.Spinbox(config_frame, from_=1, to=8, width=5, 
                                       textvariable=self.max_workers, state='disabled')
        self.workers_spin.grid(row=2, column=1, sticky=tk.W, pady=(5, 0))
    
    def create_output_section(self, parent, row):
        """Create output configuration widgets."""
        output_frame = ttk.LabelFrame(parent, text="Output Configuration", padding="10")
        output_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(0, 10))
        output_frame.columnconfigure(1, weight=1)
        
        # Output directory selection
        ttk.Label(output_frame, text="Output Directory:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        
        output_entry = ttk.Entry(output_frame, textvariable=self.output_directory, state='readonly')
        output_entry.grid(row=0, column=1, sticky="ew", padx=(0, 10))
        
        browse_btn = ttk.Button(output_frame, text="Browse", command=self.select_output_directory)
        browse_btn.grid(row=0, column=2)
        
        # Output format option
        combine_cb = ttk.Checkbutton(output_frame, text="Combine all text into single file", 
                                    variable=self.combine_output)
        combine_cb.grid(row=1, column=0, columnspan=3, sticky=tk.W, pady=(10, 0))
    
    def create_processing_section(self, parent, row):
        """Create processing control widgets."""
        process_frame = ttk.Frame(parent)
        process_frame.grid(row=row, column=0, columnspan=3, pady=(0, 10))
        
        # Start processing button
        self.process_btn = ttk.Button(process_frame, text="Start OCR Processing", 
                                     command=self.start_processing,
                                     style='Accent.TButton')
        self.process_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        # Stop processing button (initially disabled)
        self.stop_btn = ttk.Button(process_frame, text="Stop", 
                                  command=self.stop_processing, state='disabled')
        self.stop_btn.pack(side=tk.LEFT)
    
    def create_progress_section(self, parent, row):
        """Create progress display widgets."""
        progress_frame = ttk.LabelFrame(parent, text="Processing Progress", padding="10")
        progress_frame.grid(row=row, column=0, columnspan=3, sticky="nsew", pady=(0, 10))
        progress_frame.columnconfigure(0, weight=1)
        progress_frame.rowconfigure(1, weight=1)
        
        # Progress bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, 
                                           maximum=100, length=400)
        self.progress_bar.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        
        # Progress label
        self.progress_label = ttk.Label(progress_frame, text="Ready to process")
        self.progress_label.grid(row=0, column=1, padx=(10, 0))
        
        # Results text area
        self.results_text = scrolledtext.ScrolledText(progress_frame, height=10, width=80)
        self.results_text.grid(row=1, column=0, columnspan=2, sticky="nsew")
        
    def setup_drag_drop(self):
        """Setup drag and drop functionality (basic implementation)."""
        # Note: Full drag-and-drop requires tkinterdnd2 package
        # This is a placeholder for the functionality
        pass
    
    def select_files(self):
        """Open file dialog to select image files."""
        filetypes = [
            ('All supported', '*.jpg;*.jpeg;*.png;*.tiff;*.tif;*.bmp;*.pdf'),
            ('JPEG files', '*.jpg;*.jpeg'),
            ('PNG files', '*.png'),
            ('TIFF files', '*.tiff;*.tif'),
            ('BMP files', '*.bmp'),
            ('PDF files', '*.pdf'),
            ('All files', '*.*')
        ]
        
        files = filedialog.askopenfilenames(
            title="Select image files for OCR processing",
            filetypes=filetypes
        )
        
        if files:
            self.selected_files = list(files)
            self.update_file_list()
    
    def clear_files(self):
        """Clear selected files."""
        self.selected_files = []
        self.update_file_list()
    
    def update_file_list(self):
        """Update the file list display."""
        self.file_listbox.delete(0, tk.END)
        
        for file_path in self.selected_files:
            filename = Path(file_path).name
            self.file_listbox.insert(tk.END, filename)
        
        count = len(self.selected_files)
        if count == 0:
            self.file_count_label.config(text="No files selected")
        elif count == 1:
            self.file_count_label.config(text="1 file selected")
        else:
            self.file_count_label.config(text=f"{count} files selected")
    
    def select_output_directory(self):
        """Open directory dialog to select output directory."""
        directory = filedialog.askdirectory(title="Select output directory")
        if directory:
            self.output_directory.set(directory)
    
    def toggle_parallel_options(self):
        """Enable/disable parallel processing options."""
        if self.parallel_processing.get():
            self.workers_label.config(state='normal')
            self.workers_spin.config(state='normal')
        else:
            self.workers_label.config(state='disabled')
            self.workers_spin.config(state='disabled')
    
    def validate_settings(self) -> bool:
        """Validate user settings before processing."""
        if not self.selected_files:
            messagebox.showerror("Error", "Please select at least one image file.")
            return False
        
        if not self.output_directory.get():
            messagebox.showerror("Error", "Please select an output directory.")
            return False
        
        if not os.path.exists(self.output_directory.get()):
            try:
                os.makedirs(self.output_directory.get(), exist_ok=True)
            except Exception as e:
                messagebox.showerror("Error", f"Cannot create output directory: {e}")
                return False
        
        return True
    
    def start_processing(self):
        """Start the OCR processing in a separate thread."""
        if not self.validate_settings():
            return
        
        if self.is_processing:
            messagebox.showwarning("Warning", "Processing is already in progress.")
            return
        
        # Update UI state
        self.is_processing = True
        self.process_btn.config(state='disabled')
        self.stop_btn.config(state='normal')
        self.progress_var.set(0)
        self.progress_label.config(text="Initializing...")
        self.results_text.delete(1.0, tk.END)
        
        # Start processing thread
        self.processing_thread = threading.Thread(target=self.process_images, daemon=True)
        self.processing_thread.start()
    
    def stop_processing(self):
        """Stop the current processing (placeholder - actual implementation would need thread coordination)."""
        self.is_processing = False
        self.process_btn.config(state='normal')
        self.stop_btn.config(state='disabled')
        self.progress_label.config(text="Processing stopped")
        self.results_text.insert(tk.END, "\n=== Processing stopped by user ===\n")
    
    def process_images(self):
        """Process images in background thread."""
        try:
            # Create OCR processor with current settings
            dpi = None if self.dpi_setting.get() == 'auto' else int(self.dpi_setting.get())
            max_workers = self.max_workers.get() if self.parallel_processing.get() else 1
            
            processor = OCRProcessor(
                language=self.language.get(),
                enable_preprocessing=self.enable_preprocessing.get(),
                dpi=dpi,
                max_workers=max_workers
            )
            
            # Progress callback
            def progress_callback(current, total, current_file):
                progress = (current / total) * 100
                self.progress_queue.put(('progress', progress, current, total, current_file))
            
            # Add initial message
            self.progress_queue.put(('message', f"Starting OCR processing of {len(self.selected_files)} files...\n"))
            
            # Process batch
            results = processor.process_batch(
                image_paths=self.selected_files,
                output_dir=self.output_directory.get(),
                combine_output=self.combine_output.get(),
                progress_callback=progress_callback
            )
            
            # Report results
            success_count = sum(1 for result in results.values() if result == "Success")
            total_count = len(results)
            
            self.progress_queue.put(('message', f"\n=== Processing Complete ===\n"))
            self.progress_queue.put(('message', f"Successfully processed: {success_count}/{total_count} files\n"))
            
            # Report errors
            errors = {path: error for path, error in results.items() if error != "Success"}
            if errors:
                self.progress_queue.put(('message', f"\nErrors encountered:\n"))
                for path, error in errors.items():
                    filename = Path(path).name
                    self.progress_queue.put(('message', f"  {filename}: {error}\n"))
            
            self.progress_queue.put(('complete', True))
            
        except Exception as e:
            self.progress_queue.put(('error', str(e)))
            self.progress_queue.put(('complete', False))
    
    def check_progress_queue(self):
        """Check for progress updates from background thread."""
        try:
            while True:
                try:
                    message_type, *args = self.progress_queue.get_nowait()
                    
                    if message_type == 'progress':
                        progress, current, total, current_file = args
                        self.progress_var.set(progress)
                        filename = Path(current_file).name
                        self.progress_label.config(text=f"Processing {current}/{total}: {filename}")
                    
                    elif message_type == 'message':
                        message = args[0]
                        self.results_text.insert(tk.END, message)
                        self.results_text.see(tk.END)
                    
                    elif message_type == 'complete':
                        success = args[0]
                        self.is_processing = False
                        self.process_btn.config(state='normal')
                        self.stop_btn.config(state='disabled')
                        
                        if success:
                            self.progress_label.config(text="Processing completed successfully")
                            messagebox.showinfo("Success", "OCR processing completed successfully!")
                        else:
                            self.progress_label.config(text="Processing completed with errors")
                    
                    elif message_type == 'error':
                        error_msg = args[0]
                        self.results_text.insert(tk.END, f"\nERROR: {error_msg}\n")
                        self.results_text.see(tk.END)
                        messagebox.showerror("Error", f"Processing failed: {error_msg}")
                
                except queue.Empty:
                    break
        
        except Exception as e:
            print(f"Error in progress queue: {e}")
        
        # Schedule next check
        self.root.after(100, self.check_progress_queue)


def main():
    """Main function to run the GUI application."""
    root = tk.Tk()
    app = OCRBatchGUI(root)
    
    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("\nApplication interrupted by user")
    except Exception as e:
        print(f"Application error: {e}")
        messagebox.showerror("Fatal Error", f"Application encountered a fatal error: {e}")


if __name__ == "__main__":
    main()
import os
import PyPDF2
import glob
import re

def split_pdf(input_file, output_folder, pages_per_file=30):
    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    with open(input_file, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        total_pages = len(reader.pages)

        if total_pages <= 80:
            print(f"The PDF has {total_pages} pages, which is not more than 80. No splitting required.")
            return False

        for start in range(0, total_pages, pages_per_file):
            end = min(start + pages_per_file, total_pages)
            output = PyPDF2.PdfWriter()
            
            for page in range(start, end):
                output.add_page(reader.pages[page])
            
            output_filename = f"{output_folder}/split_{start+1}-{end}.pdf"
            with open(output_filename, "wb") as output_file:
                output.write(output_file)
        
        print(f"Split {total_pages} pages into {(total_pages-1)//pages_per_file + 1} files.")
        return True

def natural_sort_key(s):
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', s)]

def merge_markdown_files(input_folder, output_file):
    # Get all markdown files in the input folder
    markdown_files = sorted(glob.glob(f"{input_folder}/**/*.md", recursive=True))
    # Sort the files using natural sorting
    markdown_files.sort(key=natural_sort_key)

    # Create the output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for md_file in markdown_files:
            with open(md_file, 'r', encoding='utf-8') as infile:
                outfile.write(infile.read())
                outfile.write('\n\n')  # Add some space between merged files
    
    print(f"Merged {len(markdown_files)} markdown files into {output_file}")

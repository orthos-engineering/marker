import os
import re
from functools import lru_cache

# Compile regex patterns once and reuse them
remove_patterns = [
    re.compile(r'(?:^|\n)##\s*(?=$|\n)', flags=re.MULTILINE),
    re.compile(r'- \d+ -', flags=re.MULTILINE | re.DOTALL),
    re.compile(r'PRESIDEN REPUBLIK INDONESIA', flags=re.MULTILINE | re.DOTALL),
    re.compile(r'(?:www.bphn.go.id)', flags=re.MULTILINE | re.DOTALL),
    re.compile(r'(?:^|\n)(##\s*)?-\s*(\d{1,2})\s+(\d{1,2})\s*-', flags=re.MULTILINE | re.DOTALL),
    re.compile(r'(?:^|\n)-\d+\s-', flags=re.MULTILINE | re.DOTALL),
    re.compile(r'(?:^|\n)-\d+-', flags=re.MULTILINE | re.DOTALL),
    re.compile(r'\d+ - - ', flags=re.MULTILINE | re.DOTALL),
    re.compile(r'ww\.\s')
]

cukup_patterns = [
    re.compile(r'(?:Pasal\s+\d+\s+)?(?:Ayat|Huruf)\s+(?:\d+|\(\d+\)|[a-z])\s+Cukup (jelas|Jelas)\.|Pasal\s+\d+\s+Cukup (jelas|Jelas)?(\.)', flags=re.MULTILINE | re.DOTALL),
    re.compile(r'(?<!<<REMOVE>>)(?:Pasal\s+\d+\s+)?(?:Ayat|Huruf)\s+(?:\d+|\(\d+\)|[a-z])\s*\n?Cukup (jelas|Jelas)(?!\.)|\nPasal\s+\d+\s+Cukup (jelas|Jelas)(?!\.)(?!<<REMOVE>>)?(\.)', flags=re.MULTILINE | re.DOTALL)
]

ellipsis_pattern = re.compile(r'(.*?)(\s*(?:\.{3,}|\.\s+\.\s+\.+|\u2026)\s*)(\S+(?:\s+\S+){0,5})', flags=re.DOTALL | re.IGNORECASE)
marked_section_pattern = re.compile(r'<<REMOVE>>.*?<<REMOVE>>\n?', flags=re.DOTALL)
whitespace_pattern = re.compile(r'^\s+$', flags=re.MULTILINE)
newline_pattern = re.compile(r'\n{2,}', re.MULTILINE)


def mark_sections_for_removal(text):
    for pattern in remove_patterns:
        text = pattern.sub('<<REMOVE>>\g<0><<REMOVE>>', text)
    return text


def remove_cukup_jelas(text):
    for pattern in cukup_patterns:
        text = pattern.sub('<<REMOVE>>\g<0><<REMOVE>>', text)
    return text


@lru_cache(maxsize=1024)
def find_matching_text(before, after, max_words=3):
    # Skip empty strings
    if not before.strip() or not after.strip():
        return ''
        
    words_before = before.split()
    
    if after.lstrip().startswith('#'):
        words_after = after.lstrip().partition(' ')[2].split()
    elif after.lstrip().startswith(('REPUBLIK INDONESIA', 'PRESIDEN REPUBLIK INDONESIA')):
        words_after = after.lstrip().partition('INDONESIA')[2].lstrip().split()
    else:
        words_after = after.split()
    
    # Skip if either side has no words
    if not words_before or not words_after:
        return ''
        
    for length in range(1, min(max_words + 1, len(words_before) + 1, len(words_after) + 1)):
        if words_before[-length:] == words_after[:length]:
            return ' '.join(words_after[:length])
    
    return ''

def process_match(match):
    before, ellipsis, after = match.groups()
    
    # Skip if any part is empty
    if not before or not ellipsis or not after:
        return match.group(0)
        
    matching_text = find_matching_text(before.lower(), after.lower())
    
    if matching_text:
        try:
            before_without_match = before[:before.lower().rindex(matching_text.lower())]
            if after.lstrip().startswith('#'):
                hash_index = after.index('#')
                return f"{before_without_match.rstrip()}\n<<REMOVE>>{before[len(before_without_match):].strip()}{ellipsis}<<REMOVE>>\n{after[hash_index:].strip()}"
            elif after.lstrip().startswith(('REPUBLIK INDONESIA', 'PRESIDEN REPUBLIK INDONESIA')):
                _, _, remaining_text = after.lstrip().partition('INDONESIA')
                return f"{before_without_match.rstrip()}\n<<REMOVE>>{before[len(before_without_match):].strip()}{ellipsis}{after.split('INDONESIA')[0].strip()}<<REMOVE>>{remaining_text.lstrip()}"
            else:
                return f"{before.rstrip()}\n<<REMOVE>>{ellipsis}{matching_text}<<REMOVE>>{after[len(matching_text):].lstrip()}"
        except (ValueError, AttributeError):
            # If we can't find the matching text or any other error, just return the original text
            return match.group(0)
    
    return match.group(0)

def remove_page_break_continuations(text):
    pattern = re.compile(r'(.*?)(\s*(?:\.{3,}|\.\s+\.\s+\.+|\u2026)\s*)(\S+(?:\s+\S+){0,5})')
    return pattern.sub(process_match, text)

def remove_marked_sections(text):
    return marked_section_pattern.sub('', text)


def clean_whitespace_and_newlines(text):
    text = whitespace_pattern.sub('', text)
    text = newline_pattern.sub('\n\n', text)
    return text


def clean_text(text):
    text = mark_sections_for_removal(text)
    text = remove_page_break_continuations(text)
    text = remove_marked_sections(text)
    for i in range(2):
        text = mark_sections_for_removal(text)
        text = remove_marked_sections(text)
        text = remove_cukup_jelas(text)
        text = remove_marked_sections(text)
    return clean_whitespace_and_newlines(text)


def process_markdown_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        cleaned_content = clean_text(content)
        
        output_file_path = os.path.join('tmp/cleaned_data', os.path.basename(file_path))
        with open(output_file_path, 'w', encoding='utf-8') as file:
            file.write(cleaned_content)
        
        print(f"Processed: {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

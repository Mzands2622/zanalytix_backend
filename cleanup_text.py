import re
def clean_text(text):
    # Pattern to keep letters, numbers, parentheses, and slashes
    # [^...] matches any single character not in brackets
    cleaned_text = re.sub(r'[^a-zA-Z0-9()/\\ \-]', '', text)
    return cleaned_text


import unicodedata

def normalize_string(input_string):
    normalized_string = unicodedata.normalize('NFKD', input_string)
    ascii_string = ''.join(char for char in normalized_string if ord(char) < 128)
    return ascii_string

import hashlib

def compute_file_sha256(file_path):
    """
    Compute the SHA-256 hash of a file's contents.

    Args:
        file_path (str): Path to the file.

    Returns:
        str: SHA-256 hash of the file's contents, or None if the file does not exist.
    """
    try:
        with open(file_path, "rb") as file:
            file_contents = file.read()
            return hashlib.sha256(file_contents).hexdigest()
    except FileNotFoundError:
        return None

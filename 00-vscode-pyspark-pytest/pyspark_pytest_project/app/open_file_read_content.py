def read_file_contents(file_path):
    """
    Open a file and read its contents.

    Args:
        file_path (str): Path to the file.

    Returns:
        str: Contents of the file.
    """
    try:
        with open(file_path, "r") as file:
            return file.read()
    except FileNotFoundError:
        return None

import pytest
from unittest.mock import patch, mock_open

# Import the function to be tested
from app.open_file_read_content import read_file_contents

def test_read_file_contents():
    # Mock file contents
    mock_file_content = "This is the content of the file."

    # Patch the open function
    with patch("builtins.open", mock_open(read_data=mock_file_content)) as mocked_open:
        # Test the function
        result = read_file_contents("mock_file.txt")
        # Assert the result matches the mock content
        assert result == mock_file_content
        # Assert the file was opened correctly
        mocked_open.assert_called_once_with("mock_file.txt", "r")

def test_read_file_contents_file_not_found():
    # Patch the open function to raise a FileNotFoundError
    with patch("builtins.open", side_effect=FileNotFoundError):
        # Test the function
        result = read_file_contents("nonexistent_file.txt")
        # Assert the result is None
        assert result is None

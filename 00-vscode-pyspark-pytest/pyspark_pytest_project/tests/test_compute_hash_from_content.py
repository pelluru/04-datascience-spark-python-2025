import pytest
from unittest.mock import patch, mock_open, MagicMock

# Import the function to be tested
from app.compute_hash_from_content import compute_file_sha256

def test_compute_file_sha256():
    # Mock file content
    mock_file_content = b"This is a test file."
    mock_hash = "mocked_sha256_hash_value"

    # Patch 'open' and 'hashlib.sha256'
    with patch("builtins.open", mock_open(read_data=mock_file_content)) as mocked_open, \
         patch("hashlib.sha256") as mocked_sha256:
        # Mock the hash computation
        mock_hash_obj = MagicMock()
        mock_hash_obj.hexdigest.return_value = mock_hash
        mocked_sha256.return_value = mock_hash_obj

        # Call the function
        result = compute_file_sha256("mock_file.txt")

        # Assert the result matches the mocked hash
        assert result == mock_hash

        # Assert 'open' was called correctly
        mocked_open.assert_called_once_with("mock_file.txt", "rb")

        # Assert 'hashlib.sha256' was called
        mocked_sha256.assert_called_once()
        mock_hash_obj.hexdigest.assert_called_once()

def test_compute_file_sha256_file_not_found():
    # Patch 'open' to raise FileNotFoundError
    with patch("builtins.open", side_effect=FileNotFoundError):
        # Call the function
        result = compute_file_sha256("nonexistent_file.txt")

        # Assert the result is None
        assert result is None

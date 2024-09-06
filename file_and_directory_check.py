import os
import logging
from typing import Optional

# Create a logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a file handler to log messages to a file
file_handler = logging.FileHandler('./logs/file_check.log')
file_handler.setLevel(logging.DEBUG)

# Create a console handler to print messages to the terminal
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a formatter for the log messages
formatter = logging.Formatter('%(name)s - %(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Add the formatter to both handlers
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

def check_directory_and_file(expected_directory: str, file_name: str) -> Optional[None]:
    """
    Checks if the current working directory is the expected one and if a specific file exists in that directory.

    Args:
        expected_directory (str): The path of the expected directory.
        file_name (str): The name of the file to check for in the directory.

    Raises:
        FileNotFoundError: If the current directory is not the expected one or if the file does not exist.
        Exception: For any other unexpected errors.
    """
    try:
        # Check if in the correct directory
        current_directory = os.getcwd()
        if current_directory != expected_directory:
            raise FileNotFoundError(
                f"Current directory '{current_directory}' is not the expected directory '{expected_directory}'.")

        # Check if the file exists in the directory
        file_path = os.path.join(current_directory, file_name)
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"The file '{file_name}' does not exist in the directory '{current_directory}'.")

        logger.info("Directory and file are correct.")

    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        logger.exception("An unexpected error occurred.")


# Example usage

if __name__ == "__main__":
  expected_directory = "/path/to/your/directory"
  file_name = "your_file.txt"
  check_directory_and_file(expected_directory, file_name)

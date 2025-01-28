# PySpark Pytest Project

This is a simple project template that uses PySpark with pytest for unit testing.

## Project Structure

- **app/**: Contains the application code.
- **tests/**: Contains the test cases.
- **requirements.txt**: Lists the required dependencies.
- **.vscode/**: VS Code-specific settings.

## Setup

1. Create a virtual environment: \`python -m venv venv\`
2. Activate the virtual environment:
    - On Windows: \`venv\Scripts\activate\`
    - On macOS/Linux: \`source venv/bin/activate\`
3. Install dependencies: \`pip install -r requirements.txt\`
4. Run tests: \`pytest\`.

## Running the Application

Run the main application with:
\`python -m app.main\`.

# set asolute path 
export PYTHONPATH=$(pwd)
python -c "import app; print(app)"

# set python version same 
export PYSPARK_DRIVER_PYTHON=/opt/anaconda3/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/anaconda3/bin/python


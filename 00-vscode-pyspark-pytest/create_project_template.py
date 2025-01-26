# Project Structure
# - pyspark_pytest_project/
#   - app/
#       - __init__.py
#       - main.py
#   - tests/
#       - __init__.py
#       - test_main.py
#   - requirements.txt
#   - .vscode/
#       - settings.json
#   - README.md

# Initialize the project structure
import os

project_name = "pyspark_pytest_project"
folders = [
    f"{project_name}/app",
    f"{project_name}/tests",
    f"{project_name}/.vscode"
]

files = {
    f"{project_name}/requirements.txt": "pyspark\npytest",
    f"{project_name}/app/__init__.py": "",
    f"{project_name}/app/main.py": "from pyspark.sql import SparkSession\n\ndef create_spark_session(app_name):\n    return SparkSession.builder.appName(app_name).getOrCreate()\n\ndef main():\n    spark = create_spark_session('ExampleApp')\n    print('Spark session created:', spark)\n\nif __name__ == '__main__':\n    main()",
    f"{project_name}/tests/__init__.py": "",
    f"{project_name}/tests/test_main.py": "import pytest\nfrom app.main import create_spark_session\n\ndef test_create_spark_session():\n    app_name = 'TestApp'\n    spark = create_spark_session(app_name)\n    assert spark is not None\n    assert spark.sparkContext.appName == app_name",
    f"{project_name}/.vscode/settings.json": "{\n    \"python.pythonPath\": \"venv/bin/python\",\n    \"python.testing.pytestArgs\": [\"tests\"],\n    \"python.testing.unittestEnabled\": false,\n    \"python.testing.pytestEnabled\": true\n}",
    f"{project_name}/README.md": "# PySpark Pytest Project\n\nThis is a simple project template that uses PySpark with pytest for unit testing.\n\n## Project Structure\n\n- **app/**: Contains the application code.\n- **tests/**: Contains the test cases.\n- **requirements.txt**: Lists the required dependencies.\n- **.vscode/**: VS Code-specific settings.\n\n## Setup\n\n1. Create a virtual environment: \`python -m venv venv\`\n2. Activate the virtual environment:\n    - On Windows: \`venv\\Scripts\\activate\`\n    - On macOS/Linux: \`source venv/bin/activate\`\n3. Install dependencies: \`pip install -r requirements.txt\`\n4. Run tests: \`pytest\`.\n\n## Running the Application\n\nRun the main application with:\n\`python -m app.main\`."
}

# Create folders and files
for folder in folders:
    os.makedirs(folder, exist_ok=True)

for file_path, content in files.items():
    with open(file_path, "w") as f:
        f.write(content)

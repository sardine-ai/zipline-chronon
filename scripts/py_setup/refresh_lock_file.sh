python -m venv temp_env
source temp_env/bin/activate  # On Windows: temp_env\Scripts\activate

# Install requirements from your source requirements file
pip install -r requirements.txt

# Generate the lock file
pip freeze > requirements_lock.txt

# Clean up
deactivate
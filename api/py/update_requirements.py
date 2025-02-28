#!/usr/bin/env python3

import subprocess
import os
import sys
import tempfile

def main():
    # Get absolute path to requirements.txt
    requirements_path = os.path.abspath('api/py/requirements.txt')
    lock_path = os.path.abspath('api/py/requirements_lock.txt')

    # Create temporary virtual environment
    with tempfile.TemporaryDirectory() as temp_dir:
        venv_path = os.path.join(temp_dir, 'venv')

        # Create virtual environment
        subprocess.check_call([sys.executable, '-m', 'venv', venv_path])

        # Determine pip path
        if os.name == 'nt':  # Windows
            pip_path = os.path.join(venv_path, 'Scripts', 'pip')
        else:  # Unix/Mac
            pip_path = os.path.join(venv_path, 'bin', 'pip')

        # Install requirements
        subprocess.check_call([pip_path, 'install', '-r', requirements_path])

        # Generate lock file
        with open(lock_path, 'w') as f:
            subprocess.check_call([pip_path, 'freeze'], stdout=f)

    print(f"Lock file generated at: {lock_path}")

if __name__ == "__main__":
    main()
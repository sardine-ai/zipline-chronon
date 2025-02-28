import sys
import os

def print_path_info():
    print("Current working directory:", os.getcwd())
    print("Python path:")
    for path in sys.path:
        print("  -", path)

    # Check if ai module exists
    ai_paths = [p for p in sys.path if os.path.exists(os.path.join(p, "ai"))]
    print("\nPaths containing 'ai' directory:")
    for p in ai_paths:
        print("  -", p)
        # List contents
        ai_dir = os.path.join(p, "ai")
        print("    Contents:", os.listdir(ai_dir))

        chronon_dir = os.path.join(ai_dir, "chronon")
        if os.path.exists(chronon_dir):
            print("    ai/chronon contents:", os.listdir(chronon_dir))

            repo_dir = os.path.join(chronon_dir, "repo")
            if os.path.exists(repo_dir):
                print("    ai/chronon/repo contents:", os.listdir(repo_dir))

# Print debug info
print_path_info()
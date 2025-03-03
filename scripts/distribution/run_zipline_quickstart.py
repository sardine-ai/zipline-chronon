import tempfile
import subprocess
import os


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created temporary directory: {temp_dir}")

        quickstart_sh = os.path.join(os.path.dirname(os.path.realpath(__file__))
                                     , "run_zipline_quickstart.sh")

        # run the bash script run_zipline_quickstart.sh subprocess command
        # with the temporary directory as the argument
        subprocess.run([f"bash {quickstart_sh} {temp_dir}"], shell=True)


if __name__ == "__main__":
    main()

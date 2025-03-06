import argparse
import tempfile
import subprocess
import os

GCP = 'GCP'
AWS = 'AWS'

def main(cloud):
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created temporary directory: {temp_dir}")

        if cloud == GCP:
            # set the path to the run_gcp_quickstart.sh script
            quickstart_sh = os.path.join(os.path.dirname(os.path.realpath(__file__))
                                         , "run_gcp_quickstart.sh")
        elif cloud == AWS:
            # set the path to the run_aws_quickstart.sh script
            quickstart_sh = os.path.join(os.path.dirname(os.path.realpath(__file__))
                                         , "run_aws_quickstart.sh")

        # run the bash script run_gcp_quickstart.sh subprocess command
        # with the temporary directory as the argument
        subprocess.run([f"bash {quickstart_sh} {temp_dir}"], shell=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='Run zipline quickstart')
    parser.add_argument(
        "--cloud",
        choices=[GCP, AWS],
        help="Cloud provider to run tests on",
        required=True
    )
    args = parser.parse_args()
    main(args.cloud.upper())

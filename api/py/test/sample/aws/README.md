# AWS Zipline

## Create a Zipline project

### 1. Install an example project
```bash
# Download the sample Zipline project
aws s3 cp s3://zipline-artifacts-plaid/canary-confs-main.zip .
```
### 2. Set project configuration

Fill in `CUSTOMER_ID` key in [`common_env`](teams.json) and below. Replace [`teams.json`](../teams.json) with this file, renamed to `teams.json`.



## Install Zipline

Install the sample repository
```bash
# Create virtualenv. Only need to be done once
python3 -m venv zipline_poc
source zipline_poc/bin/activate

# Uninstall any previous version and reinstall
pip uninstall zipline-ai
aws s3 cp s3://zipline-artifacts-<CUSTOMER_ID>/release/latest/wheels/<WHEEL_FILE_NAME> .
pip install --force-reinstall <WHEEL_FILE_NAME>
```

## Run Zipline jobs

```bash
#### Add the canary-confs directory to the PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:/path/to/zipline-repo"

# Compile will convert a Zipline python definition to thrift
zipline compile --conf=joins/quickstart/training_set.py

# Run a Zipline thrift definition
#
zipline run --conf production/joins/quickstart/training_set.v1

```


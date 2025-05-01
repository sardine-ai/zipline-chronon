set -euxo pipefail
for file in api/thrift/*.thrift; do
  thrift --gen py -out api/python/ "$file"
done

SCRATCH_DIR=$(mktemp -d)
trap "rm -rf ${SCRATCH_DIR}" EXIT
ENV_VERSION=$1
VERSION=$ENV_VERSION pip wheel api/python --wheel-dir "${SCRATCH_DIR}"
cp ${SCRATCH_DIR}/zipline_ai-${ENV_VERSION}*.whl .

set -euxo pipefail
for file in api/thrift/*.thrift; do
  thrift --gen py -out api/python/ "$file"
done

VERSION=$1 pip wheel api/python

thrift --gen py -out api/py/ api/thrift/common.thrift
thrift --gen py -out api/py/ api/thrift/api.thrift
thrift --gen py -out api/py/ api/thrift/observability.thrift

VERSION=$1 pip wheel api/py

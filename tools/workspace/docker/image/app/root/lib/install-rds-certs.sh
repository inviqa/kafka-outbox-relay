#!/bin/bash

set -eu

RDS_CERT_FILE_NAME="/usr/local/share/ca-certificates/rds-ca.crt"

# install the root certificate for Amazon RDS, so that we can verify the certificate authority upon connection,
# see config/config.go
wget https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem -O ${RDS_CERT_FILE_NAME}
chmod 644 ${RDS_CERT_FILE_NAME} && update-ca-certificates

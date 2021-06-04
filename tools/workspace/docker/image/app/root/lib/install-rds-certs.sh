#!/bin/bash

set -eu

RDS_CERT_FILE_NAME="/usr/local/share/ca-certificates/rds-ca.crt"
EXPECTED_SHA1="fee42f33f2dde4e11cffe3406cf142b5f1e818bc"

# install the root certificate for Amazon RDS, so that we can verify the certificate authority upon connection,
# see config/config.go
wget https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem -O ${RDS_CERT_FILE_NAME}

ACTUAL_SHA1="$(sha1sum "${RDS_CERT_FILE_NAME}" | awk '{print $1}')"

if [ "${EXPECTED_SHA1}" != "${ACTUAL_SHA1}" ]; then
  echo "ABORTING: The downloaded RDS root cert does not match our expected checksum. Expected ${EXPECTED_SHA1} but got ${ACTUAL_SHA1}."
  exit 1
fi

chmod 644 ${RDS_CERT_FILE_NAME} && update-ca-certificates

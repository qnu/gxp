#!/bin/sh

REL_SCRIPT_DIR=`dirname $0`
SCRIPT_DIR=`(cd ${REL_SCRIPT_DIR}; pwd)`
LOG_DIR=${SCRIPT_DIR}/../logs

cd ${SCRIPT_DIR}

/usr/sbin/logrotate -s ${LOG_DIR}/logrotate.status logrotate.conf

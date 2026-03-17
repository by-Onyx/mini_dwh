#!/bin/bash
# greenplum-init.sh

export GREENPLUM_DB=${GREENPLUM_DB:-warehouse_gp}

sleep 15

psql -U ${GREENPLUM_USER} -d postgres -c "CREATE DATABASE ${GREENPLUM_DB};"
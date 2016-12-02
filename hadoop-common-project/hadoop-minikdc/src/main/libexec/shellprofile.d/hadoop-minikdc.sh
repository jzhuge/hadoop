#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [[ "${HADOOP_SHELL_EXECNAME}" = hadoop ]]; then
  hadoop_add_subcommand "minikdc <principal>+" "run MiniKdc"
fi

## @description  Run minikdc subcommand
## @audience     private
## @stability    stable
## @replaceable  no
function hadoop_subcommand_minikdc
{
  if [[ $# = 0 ]]; then
    hadoop_exit_with_usage
  fi

  WORK_DIR="${HADOOP_HOME}/minikdc"

  if [[ ! -w "${WORK_DIR}" ]] && [[ ! -d "${WORK_DIR}" ]]; then
    hadoop_error "WARNING: ${WORK_DIR} does not exist. Creating."
    mkdir -p "${WORK_DIR}" > /dev/null 2>&1
    if [[ $? -gt 0 ]]; then
      hadoop_error "ERROR: Unable to create ${WORK_DIR}. Aborting."
      exit 1
    fi
  fi

  HADOOP_SUBCMD_SUPPORTDAEMONIZATION=true
  HADOOP_CLASSNAME=org.apache.hadoop.minikdc.MiniKdc
  HADOOP_SUBCMD_ARGS=("${WORK_DIR}" "${HADOOP_CONF_DIR}/minikdc.properties"
    "${WORK_DIR}/keytab" "${@}")

  hadoop_add_classpath hadoop-minikdc
}
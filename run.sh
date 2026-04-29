#!/bin/bash
# Copyright 2026 AstroLab Software
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -e

# Grab the command line arguments
while [ "$#" -gt 0 ]; do
  case "$1" in
    -h)
      HELP_ON_SERVICE="-h"
      shift 1
      ;;
    -s)
      SCRIPT=$2
      shift 2
      ;;
    -c)
      CORES_MAX=$2
      shift 2
      ;;
    -*)
      echo "unknown option: $1" >&2
      exit 1
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

spark-submit \
    --master "${MASTER}" \
    --conf spark.executorEnv.HOME="${HOME}"\
    --conf spark.sql.execution.arrow.pyspark.enabled=true\
    --conf spark.sql.execution.arrow.maxRecordsPerBatch=1000000\
    --conf spark.kryoserializer.buffer.max=512m\
    --driver-memory 8G --executor-memory 4G \
    --conf spark.cores.max="${CORES_MAX}" --conf spark.executor.cores=2\
    "${SCRIPT}"


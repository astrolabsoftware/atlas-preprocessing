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
from atlas.definition import CAT_V3_SCHEMA
from atlas.utils import init_spark


def main():
    """dat to parquet"""
    spark = init_spark("atlas_0")
    df = (
        spark.read
        .format("csv")
        .options(delimiter=" ", header=False, comment="#", inferSchema=True)
        .load("atlas-sscat.v3.0.dat")
    )

    for old_col, new_col in zip(df.columns, CAT_V3_SCHEMA):
        df = df.withColumnRenamed(old_col, new_col)

    # FIXME: why there are additional columns with empty values?
    df = df.drop("_c22", "_c23")

    # save as parquet
    df.repartition(64).write.parquet("atlas-sscat.v3.0.parquet")


if __name__ == "__main__":
    main()

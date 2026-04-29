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
import logging

import pyspark.sql.functions as F
from fink_utils.sso.ssoft import join_aggregated_sso_data

from atlas.definition import MAPPING_CODES
from atlas.utils import init_spark, spherical_offsets_to

_LOG = logging.getLogger(__name__)


def main():
    """Merge ATLAS and ZTF"""
    spark = init_spark("atlas_3")

    # ATLAS
    files = [
        f"atlas-sscat.v3.0_x_ztf.202512_{code}_with_ephems.parquet"
        for code in MAPPING_CODES.values()
    ]

    for index, file in enumerate(files):
        if index == 0:
            df_prev = spark.read.format("parquet").load(file)
            continue
        df_new = spark.read.format("parquet").load(file)
        df_atlas_join = join_aggregated_sso_data(df_new, df_prev, on="name")
        df_prev = df_atlas_join

    # FIXME: add it to ZTF rather than dropping it from ATLAS
    df_atlas_join = df_atlas_join.drop("obscode")
    df_atlas_join = df_atlas_join.drop("ctol", "cSOE", "cjd_lc")

    # ZTF
    df_ztf = spark.read.format("parquet").load(
        "sso_ztf_lc_aggregated_202512_for_atlas.parquet"
    )
    # remove objects without ephemerides
    df_ztf = df_ztf.filter(F.col("RA").isNotNull())
    df_ztf = df_ztf.withColumnRenamed("cjd", "cjd_obs")
    df_ztf = df_ztf.drop("ssnamenr")

    # to match ATLAS/quaero convention
    df_ztf = df_ztf.withColumn("name", F.regexp_replace("name", " ", "_"))

    # Add dx, dy for ZTF
    df_ztf = df_ztf.withColumn(
        "cdxdy", spherical_offsets_to("cra", "RA", "cdec", "DEC")
    )
    df_ztf = df_ztf.withColumn("cdx", df_ztf["cdxdy"].getItem("cdx"))
    df_ztf = df_ztf.withColumn("cdy", df_ztf["cdxdy"].getItem("cdy"))
    df_ztf = df_ztf.drop("cdxdy")

    assert sorted(df_ztf.columns) == sorted(df_atlas_join.columns), (
        sorted(df_ztf.columns),
        sorted(df_atlas_join.columns),
    )

    # Join
    df_join = join_aggregated_sso_data(df_ztf, df_atlas_join, on="name")

    df_join.write.parquet("atlas-sscat.v3.0_x_ztf.202512_full_join.parquet")


if __name__ == "__main__":
    main()

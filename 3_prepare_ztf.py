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
    """Prepare ZTF"""
    spark = init_spark("atlas_3")

    # ZTF
    df_ztf = spark.read.format("parquet").load(
        "sso_ztf_lc_aggregated_202512_for_atlas.parquet"
    )
    # remove objects without ephemerides
    df_ztf = df_ztf.filter(F.col("RA").isNotNull())

    # remove objects zith suspect ephemerides
    df_ztf = df_ztf.withColumn('bad', F.when(F.size('cra') != F.size('RA'), F.lit(1)).otherwise(F.lit(0))).filter('bad = 0').drop('bad')

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
    df_ztf = df_ztf.drop('Phase', 'Elong', 'RA', 'DEC', 'px', 'py', 'pz', 'vx', 'vy', 'vz')

    df_ztf.write.parquet('atlas-sscat.v3.0_x_ztf.202512_I41.parquet')


if __name__ == "__main__":
    main()

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

from atlas.definition import COLS_OF_INTEREST, MAPPING_CODES
from atlas.utils import get_iau_codes, init_spark, mjd2jd, translate

_LOG = logging.getLogger(__name__)


def main():
    """Split per station"""
    spark = init_spark("atlas_2")

    # Full ATLAS
    df = spark.read.format("parquet").load("atlas-sscat.v3.0_with_names.parquet")
    df = df.filter(df["name"].isNotNull())

    # ZTF names
    df_ztf = spark.read.format("parquet").load(
        "sso_ztf_lc_aggregated_202512_for_atlas.parquet"
    )
    # remove objects without ephemerides
    df_ztf = df_ztf.filter(F.col("RA").isNotNull())
    df_ztf = df_ztf.select("name")

    for code in MAPPING_CODES:
        df_sub = df.filter(F.substring(df["obs"], 1, 2) == code)
        _LOG.warning(f"{code}: {df_sub.count()} observations")

        # operations
        ops = [F.collect_list(col).alias("c" + col) for col in COLS_OF_INTEREST]

        # Group by name
        df_atlas_grouped = df_sub.groupBy("name").agg(*ops)

        # Processing
        df_atlas_grouped = df_atlas_grouped.withColumn(
            "cjd_obs", mjd2jd("cMJD_obs")
        ).drop("cMJD_obs")

        df_atlas_grouped = df_atlas_grouped.withColumn(
            "cjd_lc", mjd2jd("cMJD_lc")
        ).drop("cMJD_lc")

        df_atlas_grouped = df_atlas_grouped.withColumn("cfid", translate("cfilt")).drop(
            "cfilt"
        )

        df_atlas_grouped = df_atlas_grouped.withColumn(
            "ciauobs", get_iau_codes("cobs")
        ).drop("cobs")

        # Renaming
        df_atlas_grouped = df_atlas_grouped.withColumnRenamed("cm", "cmagpsf")
        df_atlas_grouped = df_atlas_grouped.withColumnRenamed("cdm", "csigmapsf")

        # Crossmatch with ZTF
        df_join = df_atlas_grouped.join(df_ztf, on="name")

        df_join.write.parquet(
            f"atlas-sscat.v3.0_x_ztf.202512_{MAPPING_CODES[code]}.parquet"
        )


if __name__ == "__main__":
    main()

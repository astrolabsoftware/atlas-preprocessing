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

    files += ['atlas-sscat.v3.0_x_ztf.202512_I41_with_ephems.parquet"']

    for index, file in enumerate(files):
        if index == 0:
            df_prev = spark.read.format("parquet").load(file)
            continue
        df_new = spark.read.format("parquet").load(file)
        df_join = join_aggregated_sso_data(df_new, df_prev, on="name")
        df_prev = df_join

    df_join.write.parquet("atlas-sscat.v3.0_x_ztf.202512_full_join.parquet")


if __name__ == "__main__":
    main()

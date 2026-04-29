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
from atlas.utils import init_spark, rockify


def main():
    """Apply quaero"""
    spark = init_spark("atlas_1")
    df = spark.read.format("parquet").load("atlas-sscat.v3.0.parquet")
    pdf_atlas_kast = df.select("kast").distinct().toPandas()
    pdf_atlas_kast["name"] = rockify(pdf_atlas_kast["kast"])
    df_atlas_kast = spark.createDataFrame(pdf_atlas_kast)
    df_atlas_with_name = df.join(df_atlas_kast, on="kast")
    df_atlas_with_name.write.parquet("atlas-sscat.v3.0_with_names.parquet")


if __name__ == "__main__":
    main()

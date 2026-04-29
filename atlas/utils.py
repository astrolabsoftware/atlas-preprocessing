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
import numpy as np
import pandas as pd
import rocks
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType

from atlas.definition import MAPPING_BANDS, MAPPING_CODES


def init_spark(name):
    """Initialise a Spark Session"""
    spark = SparkSession.builder.appName(name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@pandas_udf(ArrayType(DoubleType()))
def mjd2jd(mjd: pd.Series) -> pd.Series:
    """NJD to JD converter for arrays"""
    return mjd.apply(lambda row: [i + 2400000.5 for i in row])


@pandas_udf(ArrayType(IntegerType()))
def translate(band: pd.Series) -> pd.Series:
    """Map bands strings to numeric"""
    return band.apply(lambda row: [MAPPING_BANDS[i] for i in row])


# HKO, MLO, CHL, STH
@pandas_udf(ArrayType(StringType()))
def get_iau_codes(obs: pd.Series) -> pd.Series:
    """Map codes to station ID"""
    return obs.apply(lambda row: [MAPPING_CODES[i[0:2]] for i in row])


def rockify(ssoid: pd.Series) -> pd.Series:
    """Extract names and numbers from SSO ID

    Parameters
    ----------
    ssoid: pd.Series of str
        SSO ID (name, designation, packed, etc.)

    Returns
    -----------
    sso_name: pd.Series of str
        SSO names according to quaero
    """
    # rockify
    names_numbers = rocks.identify(ssoid.to_numpy())

    sso_name = np.transpose(names_numbers)[0]

    return pd.Series(sso_name)

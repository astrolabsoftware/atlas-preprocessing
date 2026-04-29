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
CAT_V3_SCHEMA = [
    "MJD_obs",
    "MJD_lc",
    "H",
    "dm",
    "filt",
    "m",
    "V",
    "m1AU",
    "delta",
    "R",
    "SOE",
    "phi0",
    "phi1",
    "x",
    "y",
    "obs",
    "kast",
    "ra",
    "dec",
    "dx",
    "dy",
    "tol",
]

COLS_OF_INTEREST = [
    "MJD_obs",
    "MJD_lc",
    "filt",
    "m",
    "dm",
    "obs",
    "ra",
    "dec",
    "tol",
    "SOE",
    "dx",
    "dy",
]

MAPPING_CODES = {
    "01": "T05",
    "02": "T08",
    "03": "W68",
    "04": "M22",
}

MAPPING_BANDS = {"o": 3, "c": 4}

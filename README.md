# ATLAS catalog preprocessing

This repository contains necessary scripts to massage ATLAS Solar System Catalog for Fink processing. The input catalog can be found at: https://astroportal.ifa.hawaii.edu/atlas/sscat/README.md (current version is v3).

## DAT to Parquet

The first step is to rewrite the catalog, originally in `.dat` format to Apache Parquet format. This saves a lot of space on disk. 

```bash
./run.sh -s 0_dat_to_parquet.py -c 64
```

This will create a file `atlas-sscat.v3.0.parquet` on HDFS (about 16GB), with 258,912,463 entries.

## Name resolving

Then we need to resolve packed designation from ATLAS to SSO name using rocks:

```bash
./run.sh -s 1_resolve_names.py -c 12
```

This will create a file `atlas-sscat.v3.0_with_names.parquet` on HDFS (about 16GB). Among all entries, 258,545,151 have a non-null name (99.9%).

## Split by stations

Then we split data per station, and crossmatch with ZTF names:

```bash
./run.sh -s 2_group_by_stations_with_ztf.py -c 64
```

There are 172,941 objects in the ZTF dataset, and the number of objects per station:

```python
# Number of objects per station
for code in mapping_codes.keys():
    df_atlas_small = spark.read.format("parquet").load(f"atlas-sscat.v3.0_x_ztf.202512_{mapping_codes[code]}.parquet")
    print(mapping_codes[code], df_atlas_small.count())
T05 172928
T08 172932
W68 171994
M22 172219

# Total number of unique objects ATLAS x ZTF
container = []
for code in mapping_codes.keys():
    df_atlas_small = spark.read.format("parquet").load('sso_aggregated_ATLAS_v3_only_ztf_objects_{}'.format(mapping_codes[code]))
    pdf = df_atlas_small.select("name").toPandas()
    container = np.concatenate((container, pdf["name"].to_numpy()))
len(np.unique(container))
172922
```

This is nearly 100% of ZTF objects with ATLAS counterpart.

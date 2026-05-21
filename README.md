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

## Split by stations and restrict to objects in ZTF

Then we split data per station, and crossmatch with ZTF names (ZTF data is not yet added):

```bash
./run.sh -s 2_group_by_stations_with_ztf.py -c 64
```

This will produce 4 files, `atlas-sscat.v3.0_x_ztf.202512_{}.parquet`. There are 172,941 objects in the ZTF dataset, and the number of ATLAS x ZTF objects per station:

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

## Prepare ZTF file

You need to prepare ZTF data:

```bash
./run.sh -s 3_prepare_ztf.py -c 32
```

## ATLAS ephemerides

Now you can run ATLAS ephemerides using

```bash
cd "${FINK_HOME}"/scheduler/atlas/
nohup ./launch_ssoft.sh &
```

Do not hesitate to limit the number of objects for test purposes.

## Test data

Do not hesitate to limit the number of objects in the previous step for test purposes. Notably to test utilities in fink-utils.

```bash
# from the cluster
hdfs dfs -get atlas-sscat.v3.0_x_ztf.202512_M22_with_ephems.parquet/part-00128-9635ed35-c0d9-4e55-bc5e-09c5a8e4abaa-c000.snappy.parquet

# scp
scp username@ip:/path/*.parquet fink_utils/test_data/atlas-sscat.v3.0_x_ztf.202512_M22_with_ephems.parquet
```

## Join ATLAS and ZTF ephemerides

Now this is time to conclude by merging all ATLAS stations and adding ZTF observations to it:

```bash
./run.sh -s 4_merge_atlas_and_ztf.py -c 64
```

This will create a file `atlas-sscat.v3.0_x_ztf.202512_full_join.parquet` with both ATLAS and ZTF.

## Checks

Number of observations in total, and per station:

```python
df = spark.read.format("parquet").load("atlas-sscat.v3.0_x_ztf.202512_full_join.parquet")
df.withColumn("size", F.size("cfid")).select("size").groupBy().sum().show()
+---------+
|sum(size)|
+---------+
|230184859|
+---------+

for iauobs in ['I41', 'T05', 'T08', 'M22', 'W68']:
    flt = lambda x: x == iauobs
    df.withColumn("arr", F.filter(F.col("ciauobs"), flt)).withColumn("size", F.size("arr")).select("size").groupBy().sum().show()
+---------+                                                                     
|sum(size)|
+---------+
| 23744621|
+---------+

+---------+                                                                     
|sum(size)|
+---------+
| 70531249|
+---------+

+---------+                                                                     
|sum(size)|
+---------+
| 77262670|
+---------+

+---------+                                                                     
|sum(size)|
+---------+
| 32457914|
+---------+

+---------+                                                                     
|sum(size)|
+---------+
| 26188405|
+---------+
```

This is largely dominated by ATLAS in terms of number of observations. Here is the repartition of the number of objects per number of stations per object:

```python
df = spark.read.format("parquet").load("atlas-sscat.v3.0_x_ztf.202512_full_join.parquet")
df.withColumn("nstations", F.size(F.array_distinct("ciauobs")))\
    .groupBy("nstations").count()\
    .orderBy("nstations")\
    .show()

+---------+------+
|nstations| count|
+---------+------+
|        1|     1|
|        3|   455|
|        4|   626|
|        5|171484|
+---------+------+
```

1 objects have been seen by only one station (that's a ZTF-only object), and then most of the objects have been seen by all 5 stations (ZTF + 4 ATLAS sites).


Finally we launched the SSOFT with the SOCCA model on it!


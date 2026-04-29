# ATLAS catalog preprocessing

This repository contains necessary scripts to massage ATLAS Solar System Catalog for Fink processing. The input catalog can be found at: https://astroportal.ifa.hawaii.edu/atlas/sscat/README.md (current version is v3).

## DAT to Parquet

The first step is to rewrite the catalog, originally in `.dat` format to Apache Parquet format. This saves a lot of space on disk. 

```bash
./run.sh -s 0_dat_to_parquet.py -c 64
```

This will create a file `atlas-sscat.v3.0.parquet` on HDFS (about 16GB).

## Name resolving

Then we need to resolve packed designation from ATLAS to SSO name:

```bash
./run.sh -s 1_resolve_names.py -c 12
```

This will create a file `atlas-sscat.v3.0_with_names.parquet` on HDFS (about 16GB).

## Split by stations



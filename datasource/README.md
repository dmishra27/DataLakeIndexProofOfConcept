# Datasource

Raw source files are not meant to be committed to git from this workspace.

Download the default yearly source set with:

```powershell
python .\datasource\download_examples.py
```

By default this fetches:

- `taxi_zone_lookup.csv`
- all `yellow`, `green`, and `fhv` monthly parquet files for `2025-01` through `2025-12`

You can still request a narrower subset, for example:

```powershell
python .\datasource\download_examples.py --service yellow --service fhv --month 2025-01 --month 2025-02
```

# Avro Source

This is a custom source which reads Avro files (avsc) and register them as DataSets in DataHub.

Activate the virtual pyhton environment

```bash
python3 -m venv venv
source venv/bin/activate.fish
```


```bash
python setup.py install
```

```bash
datahub ingest -c my-source_recipe.yaml
```
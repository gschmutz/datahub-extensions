# Process Archive Source

This is a custom source which reads Avro files (avdl) from the Process Archive and register them as DataSets in DataHub.

## Important Capabilities

| Capability | Status | Notes |
|-------|-------------|--------|
| Descriptions | ok | Enabled by default | 
| [Platform Instance](https://datahubproject.io/docs/platform-instances) | ok | Enabled by default |

The ingestion can be started for all versions declared in the <archive-type>.json file, but there seem to be a problem with not all versions properly being registered in DataHub. Therefore there is the option to specify a version in the recipe, so that each version can be ingested in a separate run. 

### CLI based Ingestion

Install the Plugin

```bash
python setup.py install
```

### Starter Recipe

Check out the following recipe to get started with ingestion. See below for full configuration options. 

For general pointers on writing and running a recipe, see the [main recipe guide](https://datahubproject.io/docs/metadata-ingestion#recipes).

```yaml
source:
  type: process-archive-source.process_archive_source.ProcessArchiveSource
  config:
    env: "env"
    path: "path to json archive file"
    version:
    platform_instance: ""
    platform: "platform"
```

### Config Details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field | Description | Default |
|-------|-------------|---------|
| **env** <br>string | The environment that all assets produced by this connector belong to | `PROD` 
| **path** <br>string | File path to folder or file to ingest, or URL to a remote file. If pointed to a folder, all files with extension {file_extension} (default json) within that folder will be processed. |
| **version** <br>string | specify a single version to ingest.  |``|
| **platform_instance** <br>string | The instance of the platform that all assets produced by this recipe belong to |
| **platform** <br>string | the platform type that all assets produced belong to |  |

## Create development environment

Activate the virtual Python environment

```bash
python3 -m venv venv
source venv/bin/activate.fish
pip install -r requirements.txt
```

## Execute

```bash
datahub ingest -c process-archive_recipe.yaml
```
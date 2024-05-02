# Avro Source

This is a custom source which reads Avro files (avsc) and register them as DataSets in DataHub.

## Important Capabilities

| Capability | Status | Notes |
|-------|-------------|--------|
| Descriptions | ok | Enabled by default | 
| [Platform Instance](https://datahubproject.io/docs/platform-instances) | ok | Enabled by default |

This plugin extracts the following:

 * DataSets with its fields from an Excel Sheet


### CLI based Ingestion

#### Install the Plugin

```bash
python setup.py install
```

### Starter Recipe

Check out the following recipe to get started with ingestion. See below for full configuration options. 

For general pointers on writing and running a recipe, see the [main recipe guide](https://datahubproject.io/docs/metadata-ingestion#recipes).

```yaml
source:
  type: excel-source.excel_sheet_source.ExcelSource
  config:
    env: "env"
    path: "path-to-excel-file or path only"
    file_extension: ".xlsx"
    file_patterns:
    sheet_name: ""
    
    platform_instance: ""
    platform: "paltform"
```

### Config Details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field | Description | Default |
|-------|-------------|---------|
| **env** <br>string | The environment that all assets produced by this connector belong to | `PROD` 
| **path** <br>string | File path to folder or file to ingest, or URL to a remote file. If pointed to a folder, all files with extension {file_extension} (default json) within that folder will be processed. |
| **file_extension** <br>string | When providing a folder to use to read files, set this field to control file extensions that you want the source to process. * is a special value that means process every file regardless of extension|`.xlsx`|
| **table_pattern** <br>AllowDenyPattern | Regex patterns to filter tables for ingestion.  Specify regex to match the entire table name in `tableName` format. e.g. to match all tables starting with customer use the regex `customer.*` | `{'allow': ['.*'],` <br>`'deny': ['^_.*'],` <br>` 'ignoreCase': ...` |
| table_pattern.**ignoreCase** <br>boolean | Whether to ignore case sensitivity during pattern matching.| `True` |
| table_pattern.**allow** <br>array | List of regex patterns to include in ingestion | `['.*']` |
| table_pattern.allow.**string** <br>string | |
| table_pattern.**deny** <br>array | List of regex patterns to exclude from ingestion | `[]` |
| table_pattern.deny.**string** <br>string | || **platform_instance** <br>string | The instance of the platform that all assets produced by this recipe belong to |
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
datahub ingest -c my-source_recipe.yaml
```
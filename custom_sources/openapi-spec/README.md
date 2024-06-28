# OpenAPI Source

This is a custom source which reads and OpenAPI Sepcification and register the APIs as DataSets in DataHub.

In contrast do the standard OpenAPI source connector, this implementation uses the schema of the specification to get the information about the data returned. The official OpenAPI connector either uses examples for that or needs to call each operation of the API. 

## Important Capabilities

| Capability | Status | Notes |
|-------|-------------|--------|
| Descriptions | ok | Enabled by default | 
| [Platform Instance](https://datahubproject.io/docs/platform-instances) | ok | Enabled by default |

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
  type: openapi-spec-source.openapi_spec_source.OpenApiSpecSource
  config:
    env: "env"
    path: "path or url to OpenAPI specification"
    platform_instance: ""
    platform: "OpenApi"
```

### Config Details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field | Description | Default |
|-------|-------------|---------|
| **env** <br>string | The environment that all assets produced by this connector belong to | `PROD` 
| **api_model_path** <br>string | File path to file with the Arch Repo Model, or URL to the Arch Repo Model REST resource. |
| **api_spec_path** <br>string | File path to file with the OpenAPI Specification, or URL to an OpenAPI Specification. Supports `{system}` and `{system-component}` placeholders to refer to system/system-component being processed. Use `system` and `system_component` config fields to specify which system/system-component(s) should be processed. |
| **system** <br>string | The system to process, if left empty then all systems in the arch repo model are processed |
| **system_component** <br>string | The system component to process, if left empty, then all system components for a system are processed |
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
datahub ingest -c openapi-spec_recipe.yaml
```
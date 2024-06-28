# Arch Repo Relationships Source

This is a custom source which reads System and System Components and register them as DataFlows and DataJobs in DataHub as well as the relationships from/to a system and creates the lineage information to an API specification (dataset).

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
  type: arch-repo-source.arch_repo_ingestion_source.ArchRepoSource
  config:
    env: "env"
    api_model_path: "path to model file or URL"
    api_relationship_path: "path to get the relationships, can include <system> as a placeholder"
    provider_system: "for which system to retrieve relationships"
    dataset_name: "name of the dataset"
    platform_instance: ""
    platform: "platform"
```

### Config Details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field | Description | Default |
|-------|-------------|---------|
| **env** <br>string | The environment that all assets produced by this connector belong to | `PROD` 
| **api_model_path** <br>string | File path to folder or file to ingest, or URL to a remote file over which to retrieve the model information of the arch repo  |
| **api_relationship_path** <br>string | File path to folder or file to ingest, or URL to a remote file over which to retrieve the reliationship information of the arch repo. You can use the placeholder `<system>`, which gets automatically replaced by the given provider system to use (see next parameter). |
| **provider_system** <br>string | the system to retrieve the relationships to/from, optional, if not set all systems are retrieved |
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
datahub ingest -c my-source_recipe.yaml
```
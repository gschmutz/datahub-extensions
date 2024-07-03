# Arch Repo Script

This script supports downloading metadata from the Arch Repo REST API to files, so that it can be used to load into the DataHub using either the `openapi-spec` or `arch-repo` custom data source.

```path
~/D/G/g/d/c/a/scripts>python arch-repo.py -h                                                                                    venv 0.468s (main|✚?) 20:24
usage: arch-repo.py [-h] -mp model_url [-relurl RELATIONS_URL] [-apiurl API_SPEC_URL] -of OUTPUT_FILE [-s SYSTEM] [-sc SYSTEM_COMPONENT] [-v] command

An API for downloading Arch Repo Model, the Arch Repo Relationships or the Open API specifications.

positional arguments:
  command               the command to execute

options:
  -h, --help            show this help message and exit
  -modurl MODEL_URL, --model-url MODEL_URL
                        Specify the url to the model REST resource, if command is `download_model` or the url or a file path if the command is
                        `download_relation` or `download_api_specs`
  -relurl RELATIONS_URL, --relations-url RELATIONS_URL
                        Specify the url to the relation REST resource, if command is `download_relation`
  -apiurl API_SPEC_URL, --api-spec-url API_SPEC_URL
                        Specify the api-spec url, if command is `download_api_specs`
  -of OUTPUT_FILE, --output-file OUTPUT_FILE
                        Specify the output file, if command is `download_model` or `download_relations` or `download_api_specs`
  -s SYSTEM, --system SYSTEM
                        Specify the system to use, if command is `download_api_spec`
  -sc SYSTEM_COMPONENT, --system-component SYSTEM_COMPONENT
                        Specify the model path, if command is `download_api_spec`
  -v, --verbose         Enable verbose mode
```

## Command `download_model`

Downloads the architecture meta model to a file. 

```bash
python arch-repo.py download_relations \
						--model-url https://appplicationplatform-dev.ingress.nivel.bazg.admin.ch/applicationplatform-archrepo-service/api/model \
						--output-file /tmp/model.json 
```model-url


## Command `download_relations`

Downloads all the relations (REST, messaging) for a certain system to a file. Use the information in the model to discover the available systems. 

The following example downloads the Relations for system `DataHub`.

```bash
python arch-repo.py download_relations \
						--model-url /tmp/model.json \
						--relations-url https://appplicationplatform-dev.ingress.nivel.bazg.admin.ch/applicationplatform-archrepo-service/api/model/{system}/relations \
						--output-file /tmp/api/relations-{system}.json 
						--system DataHub
```

You can either leave `--system` empty, and the relations of all systems will be downloaded.

Use the placeholder `{system}` in the value for `--relations-url` and `--output-file` to dynamically create a value for the relevant system.

## Command `download_api_spec`

Downloads a the Open API specification for a certain system component. Use the information in the model to discover the available systems and system components. 

The following example downloads the OpenAPI specification for system `DataHub` and component `datahub-codeprovider-service`. 

```bash
python arch-repo.py download_api_spec \
						--model-url /tmp/model.json \
						--api-spec-url https://appplicationplatform-dev.ingress.nivel.bazg.admin.ch/applicationplatform-archrepo-service/api/openapi/{system}/{system-component} \
						--output-file /tmp/api/apispec-{system}-{system-component}.json 
						--system DataHub
						--system-component datahub-codeprovider-service
```

You can either leave `--system-component` empty, and the API spec of all system components for the given system will be downloaded or you can leave both `--system-component` and `--system` and then all API specs for all systems and system components will be downloaded.

Use the placeholder `{system}` and `{system-component}` in the value for `--api-spec-url` and `--output-file` to dynamically create a value for the relevant system and system component.

The script will remove parts of the OpenAPI spec which can not be handled by the parser used in the custom source.
from dataclasses import field
import json
import logging
import os
import pathlib
import subprocess
import requests
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Type
from urllib import parse

import requests
from avro import schema
from avro.datafile import DataFileReader
from avro.io import DatumReader
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import (
    make_data_platform_urn, make_dataset_urn_with_platform_instance)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.state.stale_entity_removal_handler import \
    StaleEntityRemovalSourceReport
from datahub.ingestion.source.schema_inference import avro
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata
from datahub.metadata.schema_classes import (DatasetPropertiesClass,
                                             OtherSchemaClass)
from datahub.metadata.urns import DatasetUrn
from pydantic.fields import Field

logger = logging.getLogger(__name__)

class ProcessArchiveSourceConfig(ConfigModel):
    env: str = Field("PROD",
                     description="The environment that all assets produced by this connector belong to"
    )
    path: str = Field(
        description="File path to the process archive local or URL to a remote JSON file."
    )
    version: Optional[str] = Field(
        None,
        description="The version of the schema to process."
    )
    platform: str = Field(
        description="The platform that all assets produced by this recipe belong to."
    )
    platform_instance: str = Field(
        description="The instance of the platform that all assets produced by this recipe belong to."
    )

@dataclass
class ProcessArchiveSourceReport(StaleEntityRemovalSourceReport):
    dataset_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_dataset_scanned(self, dataset: str) -> None:
        self.dataset_scanned += 1

    def report_dropped(self, table: str) -> None:
        self.filtered.append(table)
        
class ProcessArchiveSource(Source):
    source_config: ProcessArchiveSourceConfig
    report: SourceReport = SourceReport()

    def __init__(self, config: ProcessArchiveSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report: ProcessArchiveSourceReport = ProcessArchiveSourceReport()

        self.download_avro_tools()

    @classmethod
    def create(cls, config_dict, ctx):
        config = ProcessArchiveSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_report(self) -> ProcessArchiveSourceReport:
        return self.report
    
    def download_avro_tools(self):
        file_name = '/tmp/avro-tools-1.11.0.jar'
        if not os.path.exists(file_name):
            url = 'https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.11.0/avro-tools-1.11.0.jar'
            r = requests.get(url, allow_redirects=True)
            open(file_name, 'wb').write(r.content)
            
    def convert_avdl_to_avsc(self, avdl_file_name, avsc_output_dir):
        # Run a shell command
        result = subprocess.run(["java", "-jar","/tmp/avro-tools-1.11.0.jar", "idl2schemata", avdl_file_name, avsc_output_dir], capture_output=True, text=True)
        print("Return code of convert_avdl_to_avsc:", result.returncode)

    def get_process_archive_info(self, process_archive_json_file_name, config_version) -> tuple[str, str, str, str, list[str]]:
        logger.info ("Reading Process Archive info file " + process_archive_json_file_name)
        directory = os.path.dirname(process_archive_json_file_name)
        avdl_file_name_list = []
        with open(process_archive_json_file_name) as f:
            # Load JSON data from the file
            json_data = json.load(f)

            archive_type = json_data['archiveType']
            system = json_data['system']
            description = json_data['description']
            documentation_url = json_data['documentationUrl']
            for version in json_data["versions"]:
                current_version = str(version['version'])
                if config_version is None or config_version == current_version:
                    avdl_schema = os.path.join(directory, version['schema'])
                    avdl_file_name_list.append(avdl_schema)
                
        yield archive_type, system, description, documentation_url, avdl_file_name_list
                
    def get_avro_schema(self, path): 
        self.report.current_file_name = path
        path_parsed = parse.urlparse(path)
        if path_parsed.scheme not in ("http", "https"):  # A local file
            with open(path, "r") as avro_schema_file:
                avro_schema = schema.parse(avro_schema_file.read())
        else:
            try:
                avro_schema = requests.get(path)
            except Exception as e:
                raise ConfigurationError(f"Cannot read remote file {path}, error:{e}")
        return avro_schema
    
    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for archive_type, system, description, documentation_url, avdl_file_name_list in self.get_process_archive_info(self.source_config.path, self.source_config.version):

            for avdl_file_name in avdl_file_name_list:
                avsc_output_dir = '/tmp/' + avdl_file_name
                logger.info(f"Reading from {avdl_file_name}")

                self.report.report_dataset_scanned(archive_type)
                
                self.convert_avdl_to_avsc(avdl_file_name, avsc_output_dir)
                
                avro_schema = self.get_avro_schema(os.path.join(avsc_output_dir, archive_type + '.avsc'))
                fields = schema_util.avro_schema_to_mce_fields(avro_schema)
                
                platform_urn = make_data_platform_urn(self.source_config.platform)
                dataset_urn = make_dataset_urn_with_platform_instance(
                            platform=self.source_config.platform,
                            name=archive_type,
                            platform_instance=self.source_config.platform_instance,
                            env=self.source_config.env,
                )
                
                custom_properties = {"documentationUrl": documentation_url}
                dataset_properties = DatasetPropertiesClass(
                                    description=description,
                                    name=archive_type,
                                    customProperties=custom_properties,
                )
                
                # Construct a MetadataChangeProposalWrapper object.
                yield MetadataChangeProposalWrapper(
                            entityUrn=dataset_urn,
                            entityType="dataset",
                            aspect=dataset_properties
                ).as_workunit()                
                
                schema_metadata = SchemaMetadata(
                            schemaName=archive_type,
                            platform=platform_urn,
                            # version is server assigned
                            version=0,
                            hash="",
                            fields=fields,
                            platformSchema=OtherSchemaClass(rawSchema=str(avro_schema)),
                )
                
                # Construct a MetadataChangeProposalWrapper object.
                yield MetadataChangeProposalWrapper(
                                    entityUrn=dataset_urn,
                                    entityType="dataset",
                                    aspect=schema_metadata,

                ).as_workunit()

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass

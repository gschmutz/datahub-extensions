import logging
import pathlib
from typing import Iterable, Optional

from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

from datahub.ingestion.source.schema_inference import avro

from datahub.ingestion.extractor import schema_util
from avro.datafile import DataFileReader
from avro.io import DatumReader
from avro import schema

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance
)
from datahub.metadata.schema_classes import DatasetPropertiesClass
from datahub.metadata.urns import DatasetUrn
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata
from datahub.metadata.schema_classes import OtherSchemaClass
from datahub.emitter.rest_emitter import DatahubRestEmitter

from urllib import parse

import json
import os

import requests

logger = logging.getLogger(__name__)

class AvroSourceConfig(ConfigModel):
    env: str = Field("PROD",
                     description="The environment that all assets produced by this connector belong to"
    )
    path: str = Field(
        description="File path to folder or file to ingest, or URL to a remote file. If pointed to a folder, all files with extension {file_extension} (default json) within that folder will be processed."
    )
    file_extension: Optional[str] = Field(
        ".avsc",
        description="When providing a folder to use to read files, set this field to control file extensions that you want the source to process. * is a special value that means process every file regardless of extension",
    )
    dataset_name: str = Field(
        description="File path to folder or file to ingest, or URL to a remote file. If pointed to a folder, all files with extension {file_extension} (default json) within that folder will be processed."
    )
    platform: str = Field(
        description="The platform that all assets produced by this recipe belong to."
    )
    platform_instance: str = Field(
        description="The instance of the platform that all assets produced by this recipe belong to."
    )

class AvroSource(Source):
    source_config: AvroSourceConfig
    report: SourceReport = SourceReport()

    def __init__(self, config: AvroSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = AvroSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_filenames(self) -> Iterable[str]:
        path_parsed = parse.urlparse(str(self.source_config.path))
        if path_parsed.scheme in ("file", ""):
            path = pathlib.Path(self.source_config.path)
            if path.is_file():
                self.report.total_num_files = 1
                return [str(path)]
            elif path.is_dir():
                files_and_stats = [
                    (str(x), os.path.getsize(x))
                    for x in path.glob(f"*{self.source_config.file_extension}")
                    if x.is_file()
                ]
                self.report.total_num_files = len(files_and_stats)
                self.report.total_bytes_on_disk = sum([y for (x, y) in files_and_stats])
                return [x for (x, y) in files_and_stats]
            else:
                raise Exception(f"Failed to process {path}")
        else:
            self.report.total_num_files = 1
            return [str(self.source_config.path)]

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
        for path in self.get_filenames():
            
            logger.info(f"Reading from {path}")
            avro_schema = self.get_avro_schema(path)
            fields = schema_util.avro_schema_to_mce_fields(avro_schema)
            
            platform_urn = make_data_platform_urn(self.source_config.platform)
            dataset_urn = make_dataset_urn_with_platform_instance(
                        platform=self.source_config.platform,
                        name=self.source_config.dataset_name,
                        platform_instance=self.source_config.platform_instance,
                        env=self.source_config.env,
            )
            
            schema_metadata = SchemaMetadata(
                        schemaName=self.source_config.dataset_name,
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

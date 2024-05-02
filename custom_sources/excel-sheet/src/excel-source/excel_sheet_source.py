import logging
import pathlib
from typing import Dict, Iterable, List, Optional, Type

from pydantic.fields import Field
import pandas as pd

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    FixedTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)   

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

from datahub.ingestion.extractor import schema_util

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

class ExcelSourceConfig(ConfigModel):
    env: str = Field("PROD",
                     description="The environment that all assets produced by this connector belong to"
    )
    path: str = Field(
        description="File path to folder or file to ingest, or URL to a remote file. If pointed to a folder, all files with extension {file_extension} (default json) within that folder will be processed."
    )
    file_extension: Optional[str] = Field(
        ".xlsx",
        description="When providing a folder to use to read files, set this field to control file extensions that you want the source to process. * is a special value that means process every file regardless of extension",
    )
    platform: str = Field(
        description="The platform that all assets produced by this recipe belong to."
    )
    platform_instance: str = Field(
        description="The instance of the platform that all assets produced by this recipe belong to."
    )

class ExcelSource(Source):
    
    field_type_mapping: Dict[str, Type] = {
        "bool": BooleanTypeClass,
        "boolean": BooleanTypeClass,
        "NUMC": NumberTypeClass,
        "INT1": NumberTypeClass,
        "INT2": NumberTypeClass,
        "INT4": NumberTypeClass,
        "LANG": StringTypeClass,
        "TIMS": TimeTypeClass,
        "DEC": NumberTypeClass,
        "RAW": BytesTypeClass,
        "CHAR": StringTypeClass,
        "CUKY": StringTypeClass,
        "CLNT": StringTypeClass,
        "STRG": StringTypeClass,
        "DATS": DateTypeClass,
    }    
    
    source_config: ExcelSourceConfig
    report: SourceReport = SourceReport()

    def __init__(self, config: ExcelSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = ExcelSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _get_column_type(type_name: str) -> SchemaFieldDataType:
        TypeClass: Optional[Type] = ExcelSource.field_type_mapping.get(type_name)
        assert TypeClass is not None
        dt = SchemaFieldDataType(type=TypeClass())
        return dt

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
        
    def table_to_mce_field(self, row: pd.Series, table_name: str) -> SchemaField:
        field_name = row['Feldname']
        native_data_type = row['Datentyp']
        description = row['Kurztext des Datenelements']
        field = SchemaField(
                    fieldPath=field_name,
                    # Populate it with the simple native type for now.
                    nativeDataType=native_data_type,
                    type=self._get_column_type(
                        native_data_type,
                    ),
                    description=description,
                    recursive=False,
                    nullable=True,
                    isPartOfKey=False,
                    globalTags=None,
                    glossaryTerms=None,
                    jsonProps=None,
        )
        return field
        
    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for path in self.get_filenames():
            
            logger.info(f"Reading from {path}")

            platform_urn = make_data_platform_urn(self.source_config.platform)

            # Read the Excel file into a pandas DataFrame
            df = pd.read_excel(path)

            fields: List[SchemaField] = []
            current_table = None
            for index, row in df.iterrows():
                if current_table is None:                
                    current_table = row['Tabellenname']
                    
                field = self.table_to_mce_field(row, current_table)

                if current_table == row['Tabellenname'] :
                    fields.append(field)
                else:
                    current_table = row['Tabellenname']
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
                        platformSchema=OtherSchemaClass(rawSchema=None),
                    )
                    
                    # Construct a MetadataChangeProposalWrapper object.
                    yield MetadataChangeProposalWrapper(
                                entityUrn=dataset_urn,
                                entityType="dataset",
                                aspect=schema_metadata,

                    fields = []
                    fields.append(field)
                    
                    
            ).as_workunit()

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass

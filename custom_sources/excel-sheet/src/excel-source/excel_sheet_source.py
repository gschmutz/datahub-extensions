import json
import logging
import os
import pathlib
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Type
from urllib import parse

import pandas as pd
import requests
from datahub.configuration.common import AllowDenyPattern, ConfigModel
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
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass, BooleanTypeClass, BytesTypeClass, DateTypeClass,
    EnumTypeClass, FixedTypeClass, MapTypeClass, NullTypeClass,
    NumberTypeClass, RecordTypeClass, SchemaField, SchemaFieldDataType,
    SchemaMetadata, StringTypeClass, TimeTypeClass, UnionTypeClass)
from datahub.metadata.schema_classes import (DatasetPropertiesClass,
                                             OtherSchemaClass, SchemalessClass)
from datahub.metadata.urns import DatasetUrn
from pydantic.fields import Field

logger = logging.getLogger(__name__)

class ExcelSourceConfig(ConfigModel):
    env: Optional[str] = Field("PROD",
                     description="The environment that all assets produced by this connector belong to"
    )
    path: str = Field(
        description="File path to folder or file to ingest, or URL to a remote file. If pointed to a folder, all files with extension {file_extension} (default json) within that folder will be processed."
    )
    file_extension: Optional[str] = Field(
        ".xlsx",
        description="When providing a folder to use to read files, set this field to control file extensions that you want the source to process. * is a special value that means process every file regardless of extension",
    )
    sheet_name: Optional[str] = Field(
        "Sheet1",
        description="The name of the sheet in the excel which contains the data to ingest",
    )
    table_pattern: AllowDenyPattern = AllowDenyPattern(allow=[".*"], deny=["^_.*"])
    platform: str = Field(
        description="The platform that all assets produced by this recipe belong to."
    )
    platform_instance: str = Field(
        description="The instance of the platform that all assets produced by this recipe belong to."
    )

@dataclass
class ExcelSourceReport(StaleEntityRemovalSourceReport):
    tables_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_table_scanned(self, table: str) -> None:
        self.tables_scanned += 1

    def report_dropped(self, table: str) -> None:
        self.filtered.append(table)

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
        self.report: ExcelSourceReport = ExcelSourceReport()

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

    def get_report(self) -> ExcelSourceReport:
        return self.report

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
                    isPartOfKey=False
        )
        return field
        
    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for path in self.get_filenames():
            
            logger.info(f"Reading from {path}")

            platform_urn = make_data_platform_urn(self.source_config.platform)

            # Read the Excel file into a pandas DataFrame and replace all NaN with the empty string 
            df = pd.read_excel(path, sheet_name=self.source_config.sheet_name)
            df = df.fillna('')   

            fields: List[SchemaField] = []
            current = None
            for index, row in df.iterrows():
                if current is None:                
                    current = row
                    
                field = self.table_to_mce_field(row, current['Tabellenname'])

                if current['Tabellenname'] == row['Tabellenname'] :
                    fields.append(field)
                else:
                    # we have a new table, so let's use the current table to produce the MetaChangeProposal events
                    table_name = current['Tabellenname']
                    
                    self.report.report_table_scanned(table_name)
                    
                    if self.source_config.table_pattern.allowed(table_name):
                        logger.info("process table " + table_name)

                        dataset_urn = make_dataset_urn_with_platform_instance(
                            platform=self.source_config.platform,
                            name=table_name,
                            platform_instance=self.source_config.platform_instance,
                            env=self.source_config.env,
                        )
            
                        dataset_properties = DatasetPropertiesClass(
                                            description=current['Kurztext der Tabelle'],
                                            name=None,
                                            customProperties=None,
                        )
                        # Construct a MetadataChangeProposalWrapper object.
                        yield MetadataChangeProposalWrapper(
                                    entityUrn=dataset_urn,
                                    entityType="dataset",
                                    aspect=dataset_properties
                        ).as_workunit()
                                    
                        schema_metadata = SchemaMetadata(
                            schemaName=table_name,
                            platform=platform_urn,
                            # version is server assigned
                            version=0,
                            hash="",
                            fields=fields,
                            platformSchema=SchemalessClass(),
                        )
                        
                        # Construct a MetadataChangeProposalWrapper object.
                        yield MetadataChangeProposalWrapper(
                                    entityUrn=dataset_urn,
                                    entityType="dataset",
                                    aspect=schema_metadata
                        ).as_workunit()
                    else:
                        self.report.report_dropped(table_name)
                    
                    current = row
                    fields = []
                    fields.append(field)
    
    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass

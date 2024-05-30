from dataclasses import field
import json
import logging
import os
import pathlib
import subprocess
import requests
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple, Type
from urllib import parse

import requests
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import (
    make_data_platform_urn, make_dataset_urn_with_platform_instance, make_tag_urn)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.state.stale_entity_removal_handler import \
    StaleEntityRemovalSourceReport
from datahub.ingestion.source.schema_inference import avro
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    OtherSchemaClass,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    TagAssociationClass
)
from datahub.metadata.urns import DatasetUrn
from pydantic.fields import Field

from openapi_parser import parse
from openapi_parser.specification import ( Specification, Schema, Array, Object, Property, Integer, Number, String )
from openapi_parser.enumeration import DataType
    
logger = logging.getLogger(__name__)

class OpenApiSpecSourceConfig(ConfigModel):
    env: str = Field("PROD",
                     description="The environment that all assets produced by this connector belong to"
    )
    path: str = Field(
        description="File path or URL to a to an openapi specification file."
    )
    ignore_endpoints: list = Field(
        default=[], description="List of endpoints to ignore during ingestion."
    )
    platform: str = Field(
        description="The platform that all assets produced by this recipe belong to."
    )
    platform_instance: str = Field(
        description="The instance of the platform that all assets produced by this recipe belong to."
    )

@dataclass
class OpenApiSpecSourceReport(StaleEntityRemovalSourceReport):
    dataset_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_dataset_scanned(self, dataset: str) -> None:
        self.dataset_scanned += 1

    def report_dropped(self, table: str) -> None:
        self.filtered.append(table)

@config_class(OpenApiSpecSourceConfig)        
class OpenApiSpecSource(Source):
    report: SourceReport = SourceReport()

    def __init__(self, config: OpenApiSpecSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report: OpenApiSpecSourceReport = OpenApiSpecSourceReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = OpenApiSpecSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_report(self) -> OpenApiSpecSourceReport:
        return self.report
    
    def get_endpoints(self, specification: Specification) -> dict:
        """
        Get all the URLs, together with their description and the tags
        """
        url_details = {}
        for path in specification.paths:
            for operation in path.operations:
                for response in operation.responses:
                    if (response.code == 200 and response.content is not None ):
                        desc = operation.summary
                        tags = operation.tags
                        method = operation.method.value
                        schema = response.content[0].schema
                        url_details[path.url] = {"description": desc, "tags": tags, "schema": schema, "method": method}
        
        return dict(sorted(url_details.items()))

    def init_dataset(
        self, endpoint_k: str, endpoint_dets: dict
    ) -> Tuple[DatasetSnapshot, str]:
        config = self.config

        dataset_name = endpoint_k[1:].replace("/", ".")

        if len(dataset_name) > 0:
            if dataset_name[-1] == ".":
                dataset_name = dataset_name[:-1]
        else:
            dataset_name = "root"
                
        dataset_urn = make_dataset_urn_with_platform_instance(
                            platform=config.platform,
                            name=dataset_name,
                            platform_instance=config.platform_instance,
                            env=config.env,
                        )

        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[],
        )


        print (endpoint_dets["description"])
        # adding description
        dataset_properties = DatasetPropertiesClass(
            description=endpoint_dets["description"], customProperties={}
        )
        dataset_snapshot.aspects.append(dataset_properties)

        # adding tags
        tags_str = [make_tag_urn(t) for t in endpoint_dets["tags"]]
        tags_tac = [TagAssociationClass(t) for t in tags_str]
        gtc = GlobalTagsClass(tags_tac)
        dataset_snapshot.aspects.append(gtc)

        # the link will appear in the "documentation"
        # link_url = clean_url(config.url + self.url_basepath + endpoint_k)
        # link_description = "Link to call for the dataset."
        # creation = AuditStampClass(
        #     time=int(time.time()), actor="urn:li:corpuser:etl", impersonator=None
        # )
        # link_metadata = InstitutionalMemoryMetadataClass(
        #     url=link_url, description=link_description, createStamp=creation
        # )
        # inst_memory = InstitutionalMemoryClass([link_metadata])
        # dataset_snapshot.aspects.append(inst_memory)

        return dataset_snapshot, dataset_name

    #def handle_datatype(self, dataType: DataType):        

    def handle_schema(self, schema: Schema, prefix, canonical_schema: List[SchemaField]):        
        if (isinstance(schema, Array)):
            logger.info("===>Set_metadata: ARRAY ")

            full_name = prefix + '.' + 'ARRAY[]' if prefix is not None else 'ARRAY[]'
                        
            field = SchemaField(
                 fieldPath=full_name,
                 nativeDataType="array",
                 type=SchemaFieldDataTypeClass(type=ArrayTypeClass()),
                 description="",
                 recursive=False,
            )
            canonical_schema.append(field)
            
            self.handle_schema(schema.items, full_name, canonical_schema)
        elif (isinstance(schema, Object)):
            logger.info("===>Set_metadata: OBJECT ")

            full_name = prefix + '.' + 'STRUCT' if prefix is not None else 'STRUCT'
            
            field = SchemaField(
                 fieldPath=full_name,
                 nativeDataType="record",
                 type=SchemaFieldDataTypeClass(type=RecordTypeClass()),
                 description="",
                 recursive=False,
            )
            canonical_schema.append(field)
            
            for property in schema.properties:
                self.handle_schema(schema.properties, full_name, canonical_schema)

        elif (isinstance(schema, List)):
            logger.info("===>Set_metadata: LIST ")

            for item in schema:
                self.handle_schema(item, prefix, canonical_schema)

        elif (isinstance(schema, Property)):
            logger.info("===>Set_metadata: PROPERTY ")
            full_name = prefix + '.' + schema.name if prefix is not None else schema.name

            if schema.schema.type == DataType.STRING:
                nativeType = DataType.STRING.value
                fieldType = StringTypeClass()
            elif schema.schema.type == DataType.INTEGER:
                nativeType = DataType.INTEGER.value
                fieldType = NumberTypeClass()
            elif schema.schema.type == DataType.NUMBER:
                nativeType = DataType.NUMBER.value
                fieldType = NumberTypeClass()
            elif schema.schema.type == DataType.BOOLEAN:
                nativeType = DataType.BOOLEAN.value
                fieldType = BooleanTypeClass()
            else:
                nativeType = DataType.STRING.value
                fieldType = StringTypeClass()
            
            field = SchemaField(
                 fieldPath=full_name,
                 nativeDataType=nativeType,
                 type=SchemaFieldDataTypeClass(type=fieldType),
                 description=schema.schema.description,
                 recursive=False,
            )
            
            canonical_schema.append(field)

    def set_metadata(self, dataset_name: str, schema: Schema, platform: str = "api"
    ) -> SchemaMetadata:
        canonical_schema: List[SchemaField] = []

        self.handle_schema(schema, None, canonical_schema)

        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=f"urn:li:dataPlatform:{platform}",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=canonical_schema,
        )
        return schema_metadata

    def build_wu(
        self, dataset_snapshot: DatasetSnapshot, dataset_name: str
    ) -> MetadataWorkUnit:
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return MetadataWorkUnit(id=dataset_name, mce=mce)
    
    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        
        config = self.config
        
        specification = parse(config.path, strict_enum=False)

        url_endpoints = self.get_endpoints(specification)
        
        # looping on all the urls
        for endpoint_k, endpoint_dets in url_endpoints.items():
            if endpoint_k in config.ignore_endpoints:
                continue
        
            dataset_snapshot, dataset_name = self.init_dataset(
                endpoint_k, endpoint_dets
            )
            
            schema_metadata = self.set_metadata(dataset_name, endpoint_dets["schema"])
            dataset_snapshot.aspects.append(schema_metadata)        
            
            yield self.build_wu(dataset_snapshot, dataset_name)

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass

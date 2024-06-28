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
from collections import defaultdict

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
from prance import ResolvingParser

logger = logging.getLogger(__name__)

class OpenApiSpecSourceConfig(ConfigModel):
    env: str = Field("PROD",
                     description="The environment that all assets produced by this connector belong to"
    )
    api_model_path: str = Field(
        description="File path to folder or file to ingest, or URL to a remote file. If pointed to a folder, all files with extension {file_extension} (default json) within that folder will be processed."
    )
    api_spec_path: str = Field(
        description="File path or URL to a to an openapi specification file."
    )
    system: Optional[str] = Field(
        description="the name of the system to ingest openapi specs"
    )
    system_component: Optional[str] = Field(
        description="the name of the system component to ingest openapi specs"
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
    
    def get_endpoints(self, specification: dict) -> dict:
        """
        Get all the URLs, together with their description and the tags
        """
        url_details = {}
        paths = specification["paths"]
        for path in paths:
            methods = paths[path]

            for method in methods:
                method_def = methods[method]

                tags = defaultdict(list)
                desc: str = ""
                if "summary" in method_def: 
                    desc = method_def["summary"]
                # tags were causing problems therefore we removed it
                #if (method_def["tags"] is not None):
                #    tags = method_def["tags"]
                method = method

                for responseNumber in method_def["responses"]:
                    
                    response = method_def["responses"][responseNumber]
                    if (responseNumber == "200" and "content" in response):
                        for key in response["content"]:
                            schema = response["content"][key]["schema"]
                    else:
                        schema = None
                    url_details[path] = {"description": desc, "tags": tags, "schema": schema, "method": method}
        
        return dict(sorted(url_details.items()))

    def recursion_limit_handler_none(limit, refstring, recursions):
        return None

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

        return dataset_snapshot, dataset_name

    def handle_schema(self, schema: dict, property_name: str, prefix: str, canonical_schema: List[SchemaField]):
        if schema is None:
            return
        if (schema["type"] == "array"):
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
            
            self.handle_schema(schema["items"], "array_item", full_name, canonical_schema)
        elif (schema["type"] == "object"):
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

            if "properties" in schema:
                for property_name in schema["properties"]:
                    self.handle_schema(schema["properties"][property_name], property_name, full_name, canonical_schema)
            if "additionalProperties" in schema:
                self.handle_schema(schema["additionalProperties"], property_name, full_name, canonical_schema)

        elif (schema["type"] == "list"):
            print("===>Set_metadata: LIST ")

            for item in schema:
                self.handle_schema(item, "item", prefix, canonical_schema)

        else:
            if property_name is None:
                property_name = "unknown"
            logger.info("===>Set_metadata: PROPERTY: " + property_name)

            full_name = prefix + '.' + property_name if prefix is not None else property_name

            if schema["type"] == "string":
                nativeType = "string"
                fieldType = StringTypeClass()
            elif schema["type"] == "integer":
                nativeType = "integer"
                fieldType = NumberTypeClass()
            elif schema["type"] == "number":
                nativeType = "number"
                fieldType = NumberTypeClass()
            elif schema["type"] == "boolean":
                nativeType = "boolean"
                fieldType = BooleanTypeClass()
            else:
                nativeType = "string"
                fieldType = StringTypeClass()
            
            field = SchemaField(
                 fieldPath=full_name,
                 nativeDataType=nativeType,
                 type=SchemaFieldDataTypeClass(type=fieldType),
                 description=schema.get("description", ""),
                 recursive=False,
            )
            canonical_schema.append(field)

    def set_metadata(self, dataset_name: str, schema: dict, platform: str = "OpenApi"
    ) -> SchemaMetadata:
        canonical_schema: List[SchemaField] = []

        self.handle_schema(schema, None, None, canonical_schema)

        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=f"urn:li:dataPlatform:{platform}",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=canonical_schema,
        )
        return schema_metadata
        
    def get_arch_repo_json(self, path): 
        self.report.current_file_name = path
        path_parsed = parse.urlparse(path)
        if path_parsed.scheme not in ("http", "https"):  # A local file
            with open(path, "r") as arch_repo_json_file:
                arch_repo_json = json.load(arch_repo_json_file)
        else:
            try:
                response = requests.get(path)
                if (response.status_code == 200):
                    arch_repo_json = response.json()
            except Exception as e:
                raise Exception(f"Cannot read remote file {path}, error:{e}")
        return arch_repo_json
    
    def get_api_spec(self, path: str, system: str, system_component: str) -> dict:
        specification: dict = {} 
        openapi_path = path.replace("{system}", system).replace("{system-component}", system_component)
        
        print ("path to OpenAPI Spec: " + openapi_path)

        try:
            parser = ResolvingParser(openapi_path, 
                                 recursion_limit=1, 
                                 strict=False, 
                                 recursion_limit_handler=self.recursion_limit_handler_none)
            specification = parser.specification
        except Exception as e:
            print (f"Got the following error: {e}")    
        return specification

    def build_wu(
        self, dataset_snapshot: DatasetSnapshot, dataset_name: str
    ) -> MetadataWorkUnit:
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        return MetadataWorkUnit(id=dataset_name, mce=mce)
    
    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        
        config = self.config
        
        # we assume that there is only one model
        arch_repo_model_json: dict = self.get_arch_repo_json(config.api_model_path)
        for system in arch_repo_model_json["systems"]:
            system_name = system["name"]
            if (config.system is None or system_name == config.system):       
                print ("Processing System: " + system_name )
                     
                for system_component in system["systemComponents"]:
                    system_component_name = system_component["name"]
                    if (config.system_component is None or system_component_name == config.system_component):            
                        print ("Processing System-Component: " + system_component_name)
                        specification: dict = self.get_api_spec(path = config.api_spec_path, system=system_name, system_component=system_component_name)

                        if len(specification):    
                            url_endpoints = self.get_endpoints(specification)
            
                            # looping on all the urls
                            for endpoint_k, endpoint_dets in url_endpoints.items():
                                if endpoint_k in config.ignore_endpoints:
                                    continue
                            
                                dataset_snapshot, dataset_name = self.init_dataset(
                                    endpoint_k, endpoint_dets
                                )
                                
                                schema_metadata = self.set_metadata(dataset_name, endpoint_dets["schema"], platform=config.platform)
                                dataset_snapshot.aspects.append(schema_metadata)        
                                
                                yield self.build_wu(dataset_snapshot, dataset_name)

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass

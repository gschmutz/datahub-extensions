import json
import logging
import os
import pathlib
from typing import Dict, Iterable, List, Optional
from urllib import parse
from collections import defaultdict

import requests
from avro import schema
from avro.datafile import DataFileReader
from avro.io import DatumReader
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import (
    make_data_flow_urn, make_dataset_urn_with_platform_instance, make_data_job_urn)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.schema_inference import avro
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata
from datahub.metadata.schema_classes import (DatasetPropertiesClass,
                                             OtherSchemaClass,
                                             DataJobInputOutputClass,
                                             DataFlowInfoClass,
                                             DataJobInfoClass)
from datahub.metadata.urns import DatasetUrn
from pydantic.fields import Field

logger = logging.getLogger(__name__)

class ArchRepoSourceConfig(ConfigModel):
    env: str = Field("prod",
                     description="The environment that all assets produced by this connector belong to"
    )
    api_model_path: str = Field(
        description="File path to folder or file to ingest, or URL to a remote file. If pointed to a folder, all files with extension {file_extension} (default json) within that folder will be processed."
    )
    api_relationship_path: str = Field(
        description="File path to folder or file to ingest, or URL to a remote file. If pointed to a folder, all files with extension {file_extension} (default json) within that folder will be processed."
    )
    provider_system: Optional[str] = Field(
        description="the name of the provider system of get the relationships for, if empty all systems will be retrieved and used as provider systems"
    )
    platform: str = Field(
        description="The platform that all assets produced by this recipe belong to."
    )
    platform_instance: str = Field(
        description="The instance of the platform that all assets produced by this recipe belong to."
    )

class ArchRepoSource(Source):
    source_config: ArchRepoSourceConfig
    report: SourceReport = SourceReport()

    def __init__(self, config: ArchRepoSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = ArchRepoSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_api_model(self, path : str) -> Iterable[str]:
        path_parsed = parse.urlparse(path)
        if path_parsed.scheme in ("file", ""):
            path = pathlib.Path(path)
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
        
    def get_api_relation(self, path : str, system_name : str) -> Iterable[str]:
        path = path.replace("<system>", system_name)
        path_parsed = parse.urlparse(path)
        if path_parsed.scheme in ("file", ""):
            path = pathlib.Path(path)
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

    def construct_flow_workunits(self,
                                flow_urn: str,
                                flow_name: str,
                                external_url: str,
                                description: Optional[str],
                                flow_properties: Optional[Dict[str, str]] = None, 
        ) -> Iterable[MetadataWorkUnit]:
    
        yield MetadataChangeProposalWrapper(
                entityUrn=flow_urn,
                aspect=DataFlowInfoClass(
                    name=flow_name,
                    externalUrl=external_url,
                    description=description,
                    customProperties=flow_properties,
                ),
            ).as_workunit()
            
    def construct_job_workunits(self,
                                flow_urn: str,                         
                                job_urn: str,
                                job_name: str,
                                external_url: str,
                                job_type: str,
                                description: Optional[str],
                                job_properties: Optional[Dict[str, str]] = None,
                                inlets: List[str] = [],
                                outlets: List[str] = [],
#                                inputJobs: List[str] = [],
                                status: Optional[str] = None, 
        ) -> Iterable[MetadataWorkUnit]:
        if job_properties:
            job_properties = {k: v for k, v in job_properties.items() if v is not None}
        
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=job_name,
                type=job_type,
                description=description,
                customProperties=job_properties,
                externalUrl=external_url,
                status=status,
                flowUrn=flow_urn
            ),
        ).as_workunit()

    def construct_jobinout_workunits(self,
                                job_urn: str,
                                inlets: List[str] = [],
                                outlets: List[str] = [],
        ) -> Iterable[MetadataWorkUnit]:

        yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=inlets,
                    outputDatasets=outlets
                ),
            ).as_workunit()
    
    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        
        # === Process model =====
        
        # we assume that there is only one model
        model_path_json = self.get_api_model(str(self.source_config.api_model_path))[0]
        arch_repo_model_json = self.get_arch_repo_json(model_path_json)
        for system in arch_repo_model_json["systems"]:
            # Register System as Data Flow
            flow_properties = None
            flow_id = system["name"].lower()
            flow_urn = make_data_flow_urn(orchestrator=self.source_config.platform,
                                            flow_id=flow_id,
                                            cluster=self.source_config.env,
                                            platform_instance=self.source_config.platform_instance)
                                
            yield from self.construct_flow_workunits(flow_urn = flow_urn,
                                                    flow_name = system["name"],
                                                    external_url = None,
                                                    description=None,
                                                    flow_properties=flow_properties)
            
            for system_component in system["systemComponents"]:
                # Register System Component as Data Job
                job_properties = None
                job_id = system_component["name"].lower()
                job_urn = make_data_job_urn(orchestrator=self.source_config.platform,
                                            flow_id=flow_id,
                                            job_id=job_id,
                                            cluster=self.source_config.env,
                                            platform_instance=self.source_config.platform_instance)
                                
                yield from self.construct_job_workunits(flow_urn = flow_urn,
                                                    job_urn = job_urn,
                                                    job_name = system_component["name"],
                                                    external_url = None,
                                                    job_type=self.source_config.platform,
                                                    description=None,
                                                    job_properties={ "type" : system_component["type"] }
                                                    )

         # === Process relationships =====
        for system in arch_repo_model_json["systems"]:
            system_name = system["name"]
            # check if the system should be processed
            if (self.source_config.provider_system is None or system_name == self.source_config.provider_system):            
                consumer_outlets = defaultdict(list)
                provider_outlets = defaultdict(list)
                
                print(">>>> processing: " + system_name)
                
                # get the relationships for the given system
                relationship_path_json = self.get_api_relation(str(self.source_config.api_relationship_path), system_name)[0]
            
                logger.info(f"Reading from {relationship_path_json}")
                arch_repo_json = self.get_arch_repo_json(relationship_path_json)
            
                for relation in arch_repo_json:
                    
                    if (relation["relationType"] == "REST_API_RELATION"):
                    
                        dataset_urn = make_dataset_urn_with_platform_instance (platform="OpenApi", 
                                                                        name=relation["path"][1:].replace("/", "."), 
                                                                        platform_instance=self.source_config.platform_instance,
                                                                        env=self.source_config.env)
                        key = relation["consumer"] + "::" + relation["consumerSystem"]
                        consumer_outlets[key].append(dataset_urn)
                        key = relation["provider"] + "::" + relation["providerSystem"]
                        provider_outlets[key].append(dataset_urn)
                    if (relation["relationType"] == "EVENT_RELATION"):
                        None
                         
                for key in consumer_outlets:
                    
                    consumer = key.split("::")[0].lower()
                    consumer_system = key.split("::")[1].lower()
                    
                    job_urn = make_data_job_urn(orchestrator=self.source_config.platform,
                                            flow_id=consumer_system,
                                            job_id=consumer,
                                            cluster=self.source_config.env,
                                            platform_instance=self.source_config.platform_instance)

                    yield from self.construct_jobinout_workunits(job_urn = job_urn,
                                                    inlets=[],
                                                    outlets=consumer_outlets[key])
            
                for key in provider_outlets:
                    producer = key.split("::")[0].lower()
                    producer_system = key.split("::")[1].lower()
                    
                    job_urn = make_data_job_urn(orchestrator=self.source_config.platform,
                                            flow_id=producer_system,
                                            job_id=producer,
                                            cluster=self.source_config.env,
                                            platform_instance=self.source_config.platform_instance)
                    print ("key: " + key)
                    print (provider_outlets[key])
                    yield from self.construct_jobinout_workunits(job_urn = job_urn,
                                                    inlets=provider_outlets[key],
                                                    outlets=[])

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass

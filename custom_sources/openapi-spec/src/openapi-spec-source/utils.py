from typing import Iterable
import os
import pathlib
import json
import requests
from urllib import parse
import argparse

def add_version(data, version: str):
    # add the required version field
    data["info"]["version"] = version  # Specify the version you want to add

def remove_json_property(data: dict, property_name: str):
    """
    Recursively remove all keys named 'example' from the given data structure.
    """
    if isinstance(data, dict):
        # Create a list of keys to remove to avoid modifying the dictionary while iterating
        keys_to_remove = [key for key in data if key == property_name]
        for key in keys_to_remove:
            del data[key]
        
        # Recursively call the function for the remaining keys
        for key in data:
            remove_json_property(data[key], property_name)
    
    elif isinstance(data, list):
        for item in data:
            remove_json_property(item, property_name)

def remove_json_element(data: dict, elt_name: str):
    """
    Recursively remove the specific 'schema' key with the specified value from the given data structure.
    """
    if isinstance(data, dict):
        # Check if the 'schema' key exists and matches the specific value
        if elt_name in data and data[elt_name] == {"$ref": "#/components/schemas/Schema"}:
            del data[elt_name]
        
        # Recursively call the function for the remaining keys
        for key, value in list(data.items()):
            remove_json_element(elt_name, value)
    
    elif isinstance(data, list):
        for item in data:
            remove_json_element(elt_name, item)

def get_arch_repo_json(path: str): 
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
     
def get_api_spec(url: str, system: str, system_component: str): 
    url = url.replace("{system}", system)
    url = url.replace("{system-component}", system_component)
    url_parsed = parse.urlparse(url)
    if url_parsed.scheme not in ("http", "https"):  # A local file
        raise Exception(f"Must be a valid url: {url}, error:{e}")
    else:
        try:
            response = requests.get(url)
            if (response.status_code == 200):
                api_spec_json = response.json()
        except Exception as e:
            raise Exception(f"Cannot read remote file {url}, error:{e}")
    return api_spec_json

def download_model(model_path: str, model_output_file: str):
    arch_repo_model_json = get_arch_repo_json(model_path)                            
    arch_repo_model_str = json.dumps(arch_repo_model_json, indent=4)
    
    with open(model_output_file, 'w') as file:
        file.write(arch_repo_model_str) 
                       
def download_relations(model_path: str, relations_url: str, relation_output_file: str, p_system: str):
    arch_repo_model_json = get_arch_repo_json(model_path)
    for system in arch_repo_model_json["systems"]:
        system_name = system["name"]
        if (p_system is None or system_name == p_system):       
            print ("Processing System: " + system_name)
            relations_url = relations_url.replace("{system}", system_name)
            arch_repo_model_json = get_arch_repo_json(relations_url)                            
            arch_repo_model_str = json.dumps(arch_repo_model_json, indent=4)
            
            relation_output_file = relation_output_file.replace("{system}", system_name)
            with open(relation_output_file, 'w') as file:
                file.write(arch_repo_model_str) 

def download_api_specs(model_path: str, api_spec_url: str, output_file: str, p_system: str = None, p_system_component: str = None):
    arch_repo_model_json = get_arch_repo_json(model_path)
    for system in arch_repo_model_json["systems"]:
        system_name = system["name"]
        if (p_system is None or system_name == p_system):       
            print ("Processing System: " + system_name)
                     
            for system_component in system["systemComponents"]:
                system_component_name = system_component["name"]

                if (p_system_component is None or system_component_name == p_system_component):            
                    print ("Processing System-Component: " + system_component_name)

                    api_spec_json:dict = get_api_spec(api_spec_url, system_name, system_component_name)
                    
                    print ("got api spec json: " + str(len(api_spec_json)))
                    
                    add_version(api_spec_json, "1.0.0")
                    
                    remove_json_property(api_spec_json, "example")
                    remove_json_property(api_spec_json, "Schema")
                    remove_json_property(api_spec_json, "SpecifcData")
                    remove_json_element(api_spec_json, "schema")
                    remove_json_element(api_spec_json, "recommendedSchema")
                    remove_json_element(api_spec_json, "specificData")
                    
                    api_spec_json_str = json.dumps(api_spec_json, indent=4)
                    
                    api_output_file = output_file.replace("{system}", system_name).replace("{system-component}", system_component_name)
                    with open(api_output_file, 'w') as file:
                        file.write(api_spec_json_str)
                        
def main():
    parser = argparse.ArgumentParser(description="An API for downloading Arch Repo Model and Open API specifications")

    parser.add_argument('command', type=str, help='the command to execute')
    parser.add_argument('-mp', '--model-path', type=str, action='store', help='Specify the path to the model REST resource, if command is `download_model` or `download_api_specs`', required=True)
    parser.add_argument('-relurl', '--relations-url', type=str, action='store', help='Specify the path to the relation REST resource, if command is `download_relation`', required=False)
    parser.add_argument('-apiurl', '--api-spec-url', type=str, action='store', help='Specify the api-spec url, if command is `download_api_specs`', required=False)
    parser.add_argument('-of', '--output-file', type=str, action='store', help='Specify the output file, if command is `download_model` or `download_relations` or `download_api_specs`', required=True)
    parser.add_argument('-s', '--system', action='store', type=str, help='Specify the system to use, if command is `download_api_spec`', required=False)
    parser.add_argument('-sc', '--system-component', type=str, action='store', help='Specify the model path, if command is `download_api_spec`', required=False)
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose mode', required=False)

    args = parser.parse_args()
    
    if (args.command == "download_model"):
        download_model(model_path=args.model_path, model_output_file=args.output_file)
    if (args.command == "download_relations"):
        download_relations(model_path=args.model_path, relations_url=args.relations_url, relation_output_file=args.output_file, p_system=args.system)
    elif (args.command == "download_api_spec"):
        download_api_specs(model_path=args.model_path, api_spec_url=args.api_spec_url, output_file=args.output_file, p_system=args.system, p_system_component=args.system_component)
    
if __name__ == '__main__':
    main()    
from typing import Iterable
import os
import pathlib
import json
import requests
from urllib import parse
import argparse


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
    url = url.replace("<system>", system)
    url = url.replace("<system_component>", system_component)
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

def download_model(model_path: str, model_file: str):
    arch_repo_model_json = get_arch_repo_json(model_path)                            
    arch_repo_model_str = json.dumps(arch_repo_model_json, indent=4)
    
    with open(model_file, 'w') as file:
        file.write(arch_repo_model_str) 
                       
def download_api_specs(model_path: str, api_spec_url: str, output_folder: str, p_system: str = None, p_system_component: str = None):
    arch_repo_model_json = get_arch_repo_json(model_path)
    for system in arch_repo_model_json["systems"]:
        system_name = system["name"]
        if (p_system is None or system_name == p_system):       
            print ("Processing System: " + system_name)
                     
            for system_component in system["systemComponents"]:
                system_component_name = system_component["name"]

                if (p_system_component is None or system_component_name == p_system_component):            
                    print ("Processing System-Component: " + system_component_name)

                    api_spec_json = get_api_spec(api_spec_url, system_name, system_component_name)
                    api_spec_json_str = json.dumps(api_spec_json, indent=4)                      
                    
                    api_output_folder = output_folder.replace("<system>", system_name).replace("<system_component>", system_component_name)
                    with open(api_output_folder + "/raw", 'w') as file:
                        file.write(api_spec_json_str)
                        
def main():
    parser = argparse.ArgumentParser(description="An API for downloading Arch Repo Model and Open API specifications")

    parser.add_argument('command', type=str, help='the command to execute')
    parser.add_argument('-mp', '--model-path', type=str, action='store', help='Specify the model path, if command is `download_model` or `download_api_specs`', required=True)
    parser.add_argument('-mf', '--model-file', type=str, action='store', help='Specify the model file, if command is `download_model`', required=False)
    parser.add_argument('-apiurl', '--api_spec-url', type=str, action='store', help='Specify the api-spec url, if command is `download_api_specs`', required=False)
    parser.add_argument('-apiout', '--api-output-folder', type=str, action='store', help='Specify the api-spec output folder, if command is `download_api_specs`', required=False)
    parser.add_argument('-s', '--system', action='store', type=str, help='Specify the system to use, if command is `download_api_spec`', required=False)
    parser.add_argument('-sc', '--system-component', type=str, action='store', help='Specify the model path, if command is `download_api_spec`', required=False)
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose mode', required=False)

    args = parser.parse_args()
    
    if (args.command == "download_model"):
        download_model(model_path=args.model_path, model_file=args.model_file)
    elif (args.command == "download_api_spec"):
                    
        download_api_specs(model_path=args.model_path, api_spec_url=args.api_spec_url, output_folder=args.api_spec_output_folder, p_system=args.system, p_system_component=args.system_component)
    
if __name__ == '__main__':
    main()    
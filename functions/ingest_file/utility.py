import configparser
import os

def read_config():
    config = configparser.RawConfigParser()
    config_path = r'./config.ini'
    with open(config_path, 'r') as f:
        config_string = '[dummy_section]\n' + f.read()
        config.read_string(config_string)
        details_dict = dict(config.items("dummy_section"))
        return details_dict

config = read_config()

print(config)
print(config[r'devshell_project_id'])
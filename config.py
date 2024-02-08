import json

config_g = {}

def get_config(config_path: str):
	# Read and parse config
	global config_g
	with open(config_path, 'r') as file:
		cfg = json.load(file)
		config_g.update(cfg["programs"])


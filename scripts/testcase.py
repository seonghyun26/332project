import json

MAX_WORKERS = 9

class ConfigException(Exception):
    def __init__(self, message: str):
        self.message = message

    def __str__(self):
        return self.message

class TestcaseException(Exception):
    def __init__(self, message: str):
        self.message = message

    def __str__(self):
        return self.message

class Testcase:
    def __init__(self, config_file_path: str):
        config = json.load(open(config_file_path))
        try:
            self._validate_config(config)
        except ConfigException as e:
            raise TestcaseException(f'[{config_file_path}]: {e}')
        self.config = {
            'name': config['name'],
            'workers': [ { 'index': worker['index'], 'blocks': worker['blocks'] } for worker in config['workers']
            ]
        }

    @staticmethod
    def _validate_config(config: dict):
        if 'name' not in config:
            raise ConfigException('Testcase name not specified')
        if 'workers' not in config:
            raise ConfigException('Testcase workers not specified')
        if not isinstance(config['workers'], list):
            raise ConfigException('Testcase workers must be a list')
        if len(config['workers']) == 0:
            raise ConfigException('Worker list should not be empty')
        for worker in config['workers']:
            if 'index' not in worker:
                raise ConfigException('Worker index not specified')
            if not isinstance(worker['index'], int):
                raise ConfigException('Worker index must be an integer')
            if not (0 <= worker['index'] < MAX_WORKERS):
                raise ConfigException(f'Worker index must be between 0 and {MAX_WORKERS - 1}')
            if 'blocks' not in worker:
                raise ConfigException('Worker blocks not specified')
            if not isinstance(worker['blocks'], list):
                raise ConfigException('Worker blocks must be a list')
            if len(worker['blocks']) == 0:
                raise ConfigException('Worker blocks should not be empty')
            for num_blocks in worker['blocks']:
                if not isinstance(num_blocks, int):
                    raise ConfigException('Worker blocks must be a list of integers')
                if num_blocks <= 0:
                    raise ConfigException('Worker blocks must be a list of positive integers')
        if len(set(worker['index'] for worker in config['workers'])) != len(config['workers']):
            raise ConfigException('Worker indices must be unique')

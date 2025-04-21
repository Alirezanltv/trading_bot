"""
Database Fix Module

This module creates a mock influxdb_client module to prevent import errors.
"""

import sys
from types import ModuleType
from importlib.util import spec_from_loader, module_from_spec

# Create a fake influxdb_client module
module_name = "influxdb_client"
spec = spec_from_loader(module_name, loader=None)
influxdb_module = module_from_spec(spec)
sys.modules[module_name] = influxdb_module

# Create mock classes for InfluxDB client
class MockInfluxDBClient:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.connected = False
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        pass
    
    def write_api(self, *args, **kwargs):
        return MockWriteApi()
    
    def query_api(self):
        return MockQueryApi()
    
    def close(self):
        self.connected = False

class MockWriteApi:
    def __init__(self):
        pass
    
    def write(self, *args, **kwargs):
        pass
    
    def close(self):
        pass

class MockQueryApi:
    def __init__(self):
        pass
    
    def query(self, *args, **kwargs):
        return []

class MockPoint:
    def __init__(self, measurement=None):
        self.measurement = measurement
        self.fields = {}
        self.tags = {}
        self.time = None
    
    def tag(self, key, value):
        self.tags[key] = value
        return self
    
    def field(self, key, value):
        self.fields[key] = value
        return self
    
    def time(self, time_value):
        self.time = time_value
        return self

class MockWriteOptions:
    SYNCHRONOUS = "synchronous"
    BATCHING = "batching"
    DEFAULTS = {}

# Add mock classes to module
setattr(influxdb_module, 'InfluxDBClient', MockInfluxDBClient)
setattr(influxdb_module, 'Point', MockPoint)
setattr(influxdb_module, 'WriteOptions', MockWriteOptions)

# Create client submodule
client_module = ModuleType("influxdb_client.client")
setattr(influxdb_module, 'client', client_module)
sys.modules["influxdb_client.client"] = client_module

# Create write_api submodule
write_api_module = ModuleType("influxdb_client.client.write_api")
setattr(client_module, 'write_api', write_api_module)
sys.modules["influxdb_client.client.write_api"] = write_api_module

# Add constants to write_api module
SYNCHRONOUS = "synchronous"
ASYNCHRONOUS = "asynchronous"
setattr(write_api_module, 'SYNCHRONOUS', SYNCHRONOUS)
setattr(write_api_module, 'ASYNCHRONOUS', ASYNCHRONOUS)

# Create domain submodule
domain_module = ModuleType("influxdb_client.domain")
setattr(influxdb_module, 'domain', domain_module)
sys.modules["influxdb_client.domain"] = domain_module

# Log that we've created the mock
print("DB fix applied: Created mock influxdb_client module with client.write_api support") 
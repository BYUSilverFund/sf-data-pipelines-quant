import os 
import importlib

pkg_dir = os.path.dirname(__file__)
for file in os.listdir(pkg_dir):
    if file.endswith(".py") and file not in ["__init__.py", "base.py"]:
        module_name = file[:-3]
        importlib.import_module(f".{module_name}", package=__package__)
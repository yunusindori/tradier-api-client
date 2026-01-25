"""
Install the client
"""
from setuptools import setup, find_packages
import re, pathlib

def read_version():
    init_py = pathlib.Path(__file__).parent / "tradier_api_client" / "__init__.py"
    m = re.search(r'__version__\s*=\s*"([^"]+)"', init_py.read_text(encoding="utf-8"))
    if not m:
        raise RuntimeError("Cannot find __version__ in tradier_api_client/__init__.py")
    return m.group(1)

setup(
    name='tradier_api_client',
    version=read_version(),
    packages=find_packages()
)

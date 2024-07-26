from setuptools import setup, find_packages

setup(
    name='dataflow_custom_dependencies',
    version='0.1',
    install_requires=[
        'pandas',
        'fsspec',
        'gcsfs',
        'openpyxl'
    ],
    packages=find_packages()
)

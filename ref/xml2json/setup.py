from setuptools import setup

setup(
    name='xml_to_json',
    version='0.1.0',
    py_modules=['xml_to_json'],
    install_requires=[
        'lxml',
        'pathos',
    ],
    entry_points={
        'console_scripts': [
            'xml_to_json = xml_to_json:run_from_cmd',
        ],
    }
)


from setuptools import setup, find_packages


setup(
    name='unhcr_module',
    version='0.4.7',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy',
        'requests',
        'pytz',
        'python-dotenv',
        'pandas',
        'mysqlclient',
        'boto3',
        'deepdiff'
    ],
    entry_points={
        'console_scripts': [
            'unhcr_module = unhcr.__main__:main',
        ],
    },
)

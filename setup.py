
from setuptools import setup, find_packages
with open("fedotreqs.txt") as f:
    requirements = f.read().splitlines()

setup(
    name='unhcr_module',
    version='0.4.7',
    packages=find_packages(),
    install_requires=requirements,
    package_data={'': ['migrations/*', 'migrations/**/*']}, # Include migrations
    entry_points={
        'console_scripts': [
            'unhcr_module = unhcr.__main__:main',
        ],
    },
)


from setuptools import setup, find_packages

"""
    Overall Comments:

The pull request appears to be empty or the diff is not properly displayed. Please ensure the changes are properly included in the PR for review.
Here's what I looked at during the review
🟢 General issues: all looks good
🟢 Security: all looks good
🟢 Testing: all looks good
🟢 Complexity: all looks good
🟢 Documentation: all looks good
"""
setup(
    name='unhcr_module',
    version='0.4.5',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy',
        'requests',
        'pytz',
        'python-dotenv',
        'pandas',
        'mysqlclient',
        'boto3'
    ],
    entry_points={
        'console_scripts': [
            'unhcr_module = unhcr.__main__:main',
        ],
    },
)

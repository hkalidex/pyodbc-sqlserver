import re
from setuptools import setup

with open('requirements.txt') as handle:
    contents = handle.read().split('\n')

requires = []
links = []
regex = '.*#egg=(?P<package>[A-Za-z]+).*'
for content in contents:
    match = re.match(regex, content)
    if match:
        package = match.group('package')
        requires.append(package)
        links.append(content.replace('-e ', ''))
    else:
        requires.append(content)

print('requires: {}'.format(requires))
print('links: {}'.format(links))

setup(
    name='sqlserver',
    version='0.0.2',
    author='Ankur Jain',
    author_email='ankurx.a.jain@intel.com',
    package_dir={
        '': 'src/main/python'
    },
    packages=[
        'sqlserver'
    ],
    url='https://github.intel.com/Intel-Cloud-Metrics/pyodbc-sqlserver',
    description='Python classes to interact with sql server',
    install_requires=requires,
    dependency_links=links
)

from setuptools import setup

setup(
    name='samplingFdw',
    version='0.0.1',
    author='Lee Ehudin',
    packages=['samplingFdw'],
    install_requires=['Multicorn', 'psycopg2>=2.6.2'],
    dependency_links=[
        "http://github.com/Kozea/Multicorn/tarball/master#egg=Multicorn-1.3.2"
    ])

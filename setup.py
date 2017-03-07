from setuptools import setup

setup(
    name='samplingfdw',
    version='0.0.1',
    author='Lee Ehudin',
    packages=['samplingfdw'],
    install_requires=['Multicorn', 'psycopg2>=2.6.2'],
    dependency_links=[
        "http://github.com/Kozea/Multicorn/tarball/master#egg=Multicorn-1.3.2"
    ])

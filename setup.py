from setuptools import setup, find_packages
setup(
   name='pyspark_code_builder',
   version='0.0.2',
   packages=find_packages(include=['transform','dataio',]),
   license='NA'
)
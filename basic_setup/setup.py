from setuptools import setup

with open("requirements.txt") as f:
    requirements = f.readlines()

setup(
    name = "basic_setup"
    ,version = "1.0"
    ,description = "Pipeline with dependencies"
    ,packages = ["my_package"]
    ,install_requires = requirements
)
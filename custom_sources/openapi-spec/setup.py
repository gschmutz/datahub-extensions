from setuptools import find_packages, setup

setup_output = setup(
    name="openapi-spec",
    version="1.0",
    description="Ingest OpenAPI Specifications as DataSets",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=["openapi3-parser"],
)

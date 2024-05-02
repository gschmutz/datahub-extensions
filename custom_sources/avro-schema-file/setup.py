from setuptools import find_packages, setup

setup_output = setup(
    name="avro_ingestion_source",
    version="1.0",
    description="Ingest Avro Schemas as DataSets",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=["acryl-datahub","avro"],
)

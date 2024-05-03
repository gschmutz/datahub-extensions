from setuptools import find_packages, setup

setup_output = setup(
    name="process_archive_source",
    version="1.0",
    description="Ingest Process Archive schemas as DataSets",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=["acryl-datahub","avro"],
)

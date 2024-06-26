from setuptools import find_packages, setup

setup_output = setup(
    name="arch_repo_ingestion_source",
    version="1.0",
    description="Ingest Arch Repo Data as DataJobs",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=["acryl-datahub"],
)

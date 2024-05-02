from setuptools import find_packages, setup

setup_output = setup(
    name="excel_sheet_source",
    version="1.0",
    description="Ingest Excel Sheet data as DataSets",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=["acryl-datahub","pandas","openpyxl"],
)

from os import getenv

import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

requirements_list = ["pymongo==4.3.2", "dnspython==2.2.1"]

LIB_NAME: str = "infra-mongodb-python"

env_version = getenv("VERSION", "1.0.0")

VERSION = env_version.split(".")
__version__ = VERSION
__version_str__ = ".".join(map(str, VERSION))

setuptools.setup(
    name=LIB_NAME,
    version=__version_str__,
    author="Luis Gerardo Fosado Baños",
    author_email="yeralway1@gmail.com",
    description="Mongo Manager Domain Library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=f"https://github.com/GerardoX1/{LIB_NAME}.git",
    include_package_data=True,
    keywords="infra, mongodb, library, python",
    packages=setuptools.find_packages(),
    package_data={"": ["*.json"]},
    namespace_packages=["infra"],
    install_requires=requirements_list,
    classifiers=["Programming Language :: Python :: 3"],
    python_requires=">=3.7",
    zip_safe=True,
    test_suite="tests",
)

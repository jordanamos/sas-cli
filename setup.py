from setuptools import setup

VERSION = "1.0.0"

with open("README.md", "r") as readme_file:
    readme = readme_file.read()

setup(
    name="sas_cli",
    version=VERSION,
    description="Run a SAS program from the command line",
    author="Jordan Amos",
    author_email="jordan.amos@gmail.com",
    license="MIT",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/jordanamos/sas-cli",
    packages=["sas_cli"],
    entry_points={"console_scripts": ["sas = sas_cli.__main__:main"]},
    python_requires=">=3.10",
    install_requires=[
        "numpy==1.23.5",
        "pandas==1.5.1",
        "python-dateutil==2.8.2",
        "pytz==2022.6",
        "saspy==4.4.0",
        "six==1.16.0",
    ],
    keywords="sas cli",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python",
        "Intended Audience :: Developers",
        "Development Status :: 2 - Pre-Alpha",
    ],
)

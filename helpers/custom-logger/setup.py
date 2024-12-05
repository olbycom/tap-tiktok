from setuptools import find_packages, setup

setup(
    name="custom-logger",
    packages=find_packages(include=["custom_logger"]),
    version="0.0.1",
    description="Utility class for custom logger",
    install_requires=[],
    author="Vinicius Justo (vinicius@nekt.ai)",
)

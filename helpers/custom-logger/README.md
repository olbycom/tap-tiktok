# Nekt Connector Utils

## Overview

`custom_logger` is a Python library developed for internal use at Nekt. It provides an utility class to cleanse records that don't comply with their given schema before emitting downstream for further processing.

## Building the Library

To build the library, navigate to the library's root directory and run the following command:

`python3 setup.py bdist_wheel`

This command will generate a .whl file, which can be found in the dist directory within the library folder.

## Installing the Library

We use Poetry for dependency management in our projects. To add `record_cleanser` to your project, navigate to your project's root directory and run:

`poetry add ../../helpers/custom-logger/dist/custom_logger-0.0.X-py3-none-any.whl`

## Reporting Issues

If you encounter any issues or have suggestions for improvements, please report them to the internal issue tracker or contact the maintainer.

## Contact

For any questions or further information, please contact the maintainer:

Name: Vinicius Justo

Email: <vinicius@nekt.ai>

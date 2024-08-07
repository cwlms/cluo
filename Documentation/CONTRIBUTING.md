# Contributing

## Issues
Currently, we are using the following [Asana Board](https://app.asana.com/0/1201441776830592/board) to manage issues and workflow.

## Tooling
This project uses the following tooling:  
[poetry](https://python-poetry.org/docs/configuration/) for package/dependency management  
[black](https://github.com/psf/black) for formatting  
[mypy](https://mypy.readthedocs.io/en/stable/) for static type-checking  
[flake8](https://flake8.pycqa.org/en/latest/) for linting  
[pre-commit](https://pre-commit.com/) for pre-commit hooks  
[pdoc](https://pdoc.dev/) for documentation

## Python version
cluo currently only supports Python 3.10. Consider using [pyenv](https://github.com/pyenv/pyenv) for managing Python versions.
## Development setup
```sh
pip install poetry
poetry config experimental.new-installer false
poetry install
poetry shell
```
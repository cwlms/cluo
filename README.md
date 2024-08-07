<div align="center">
  <h1>cluo</h1>

<p align="center">

<a href="https://github.com/techstars/cluo/actions/workflows/code_checks.yml/badge.svg">
    <img src="https://github.com/techstars/cluo/actions/workflows/code_checks.yml/badge.svg" alt="code_checks" />
</a>
<a href="https://github.com/techstars/cluo/actions/workflows/run_tests.yml/badge.svg">
    <img src="https://github.com/techstars/cluo/actions/workflows/run_tests.yml/badge.svg" alt="run_tests" />
</a>
<a href="https://github.com/techstars/cluo/actions/workflows/build_docs.yml/badge.svg">
    <img src="https://github.com/techstars/cluo/actions/workflows/build_docs.yml/badge.svg" alt="build_docs" />
</a>
<a href="https://img.shields.io/badge/python-3.11-blue.svg">
    <img src="https://img.shields.io/badge/python-3.11-blue.svg" alt="Python 3.11" />
</a>
<a href="https://img.shields.io/badge/code%20style-black-000000.svg">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Code style: black" >
</a>
<p>Cluo is a stage-based, composable data stream processing library in Python.</p>
</div>

## Documentation
For information about this project, build the docs with `make docs` then open `./_docs/index.html` in your browser. To do this, you will need to set up an environment with the necessary dependencies first:

```sh
make setup
```

Alternatively, you can view the Markdown files in `./Documentation/` directly.

For M1, you may need to run the following before doing anything with poetry:

```sh
brew install librdkafka
C_INCLUDE_PATH=/usr/local/Cellar/librdkafka/0.11.0/include LIBRARY_PATH=/usr/local/Cellar/librdkafka/0.11.0/lib
```

## Sample pipeline
```sh
docker build . -t cluo-image
docker run cluo-image python examples/dummy_bitshift.py
```

## Coverage report
```sh
poetry run coverage html
```
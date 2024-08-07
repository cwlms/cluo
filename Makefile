VENV_DIR=.venv

setup:
	@if [ ! -d "${VENV_DIR}" ]; then \
		echo "creating virtual environment in ${VENV_DIR}/"; \
		python3.11 -m venv ${VENV_DIR}; \
		echo "installing requirements"; \
		${VENV_DIR}/bin/pip install -qq --upgrade pip; \
		${VENV_DIR}/bin/pip install -qq --upgrade poetry==1.8.3; \
		${VENV_DIR}/bin/poetry install; \
	fi

lock: setup
	${VENV_DIR}/bin/poetry lock --no-update

install: setup
	${VENV_DIR}/bin/poetry install

docs:
	${VENV_DIR}/bin/poetry run pdoc --docformat google -o _docs/ --logo "https://static.wikia.nocookie.net/newersupermariobroswii/images/b/bf/Warp_pipe.png/revision/latest/scale-to-width-down/200?cb=20181227070915" Documentation cluo

test:
	${VENV_DIR}/bin/poetry run pytest --cov --cov-report=term-missing:skip-covered

test-coverage:
	${VENV_DIR}/bin/poetry run pytest --cov-report=html --cov

ruff-check:
	${VENV_DIR}/bin/poetry run ruff check --fix

ruff-format:
	${VENV_DIR}/bin/poetry run ruff format

mypy:
	${VENV_DIR}/bin/poetry run mypy cluo/ examples/

interrogate:
	${VENV_DIR}/bin/poetry run interrogate -c pyproject.toml cluo/

xenon:
	${VENV_DIR}/bin/poetry run xenon -b B -m A -a A cluo/

validate_examples:
	${VENV_DIR}/bin/poetry run python tools/validate_examples.py

full_lint:
	make ruff-check
	make ruff-format

full_analysis:
	make mypy
	make interrogate
	make xenon

full_check:
	make full_lint
	make full_analysis
	make validate_examples
	make test

# run all pytest tests marked with @pytest.mark.single
single-test:
	${VENV_DIR}/bin/poetry run pytest -s -vv -m single

.PHONY: .test_requirements test cov install dist upload example clean

RM = rm -rf
PYTHON = python3


# ┏━━━━━━━━━┓
# ┃ Testing ┃
# ┗━━━━━━━━━┛

PYTEST = $(PYTHON) setup.py test
PYTEST_COV_OPTS ?= --cov=src --cov-report=term --no-cov-on-fail


test: cleanpy
	$(PYTEST)


cov: cleanpy
	$(PYTEST) $(PYTEST_COV_OPTS)


testdep:
	@echo Installing && pip3 install --quiet --upgrade --upgrade-strategy eager -e .[test] && echo 'OK' || echo 'FAILED'


testclean: cleanpy
	@$(RM) .cache .coverage


# ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
# ┃ Installing, building, running ┃
# ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

install: cleanpy
	@echo -n 'Installing... '
	@pip3 install --quiet --upgrade --upgrade-strategy eager -e . && echo 'OK' || echo 'FAILED'


# ┏━━━━━━━━━━━━━┓
# ┃ Cleaning up ┃
# ┗━━━━━━━━━━━━━┛

cleanpy:
	@echo Removing pyc and pyo files
	@find . -type f -name '*.py[co]' -exec rm {} \;

clean:
	@# From running pytest:
	$(RM) .coverage .cache

	@# From `pip3 install -e .`:
	-pip3 uninstall -y timetell-importer && $(RM) src/timetell_importer.egg-info

	@# From `setup.py sdist`:
	$(RM) dist

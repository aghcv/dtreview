PYTHON ?= python3

.PHONY: all analysis test manuscript

all: analysis test manuscript

analysis:
	MPLCONFIGDIR=.cache/matplotlib $(PYTHON) analysis/generate_outputs.py --check

test:
	MPLCONFIGDIR=.cache/matplotlib $(PYTHON) -m unittest discover -s tests

manuscript: analysis
	mkdir -p build
	tectonic -X compile main.tex --outdir build --keep-logs

INPUT ?= preliminary-report.md
OUTPUT ?= build/$(basename $(notdir $(INPUT))).pdf

.PHONY: pdf figures

pdf:
	python3 rendering/build.py "$(INPUT)" --output "$(OUTPUT)"

figures:
	python3 assets/gen_ring_example.py

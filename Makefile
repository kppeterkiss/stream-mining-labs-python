PYTHON ?= 3.11
UV ?= uv
VENV_PATH ?= .venv

.PHONY: help bootstrap venv sync setup-env doctor

help:
	@echo "Targets:"
	@echo "  make bootstrap   Create venv + install locked deps with uv"
	@echo "  make venv        Create .venv with Python $(PYTHON)"
	@echo "  make sync        Install dependencies from uv.lock"
	@echo "  make setup-env   Print command to export JAVA_HOME and PYTHONPATH"
	@echo "  make doctor      Check toolchain basics"

bootstrap: venv sync
	@echo "Bootstrap complete."
	@echo "Run: source scripts/setup_env.sh"

venv:
	$(UV) venv --python $(PYTHON) $(VENV_PATH)

sync:
	$(UV) sync

setup-env:
	@echo "source scripts/setup_env.sh"

doctor:
	@command -v $(UV) >/dev/null || (echo "uv not found" && exit 1)
	@$(UV) --version
	@/usr/libexec/java_home >/dev/null && echo "JAVA_HOME can be resolved" || (echo "JAVA_HOME lookup failed" && exit 1)
	@echo "Python target: $(PYTHON)"

.PHONY: release-readiness release-readiness-ci nightly-quality perf-gate criterion-gate docs-build docs-serve

# Full shipping gate. Keeps cargo artifacts in target/ for cache reuse.
release-readiness:
	./scripts/release_readiness.sh

# CI variant uses tighter baseline profile.
release-readiness-ci:
	PERF_BASELINE_PROFILE=ci ./scripts/release_readiness.sh

nightly-quality:
	./scripts/soak_slo_gate.sh
	./scripts/perf_regression_gate.sh

perf-gate:
	./scripts/perf_regression_gate.sh

criterion-gate:
	./scripts/criterion_gate.sh

docs-build:
	cd docs-site && mdbook build

docs-serve:
	cd docs-site && mdbook serve --hostname 127.0.0.1 --port 3000 --open

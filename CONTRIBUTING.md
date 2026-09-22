# Contributing to FinSight

Thanks for your interest in improving FinSight. Bug reports, new providers, new datasets, and
documentation fixes are all welcome.

## Development setup

```bash
git clone https://github.com/YaseenSadat/FinSight.git
cd FinSight
make dev            # creates .venv, installs FinSight with dev extras, installs pre-commit hooks
source .venv/bin/activate
```

## Checks

Run these before opening a pull request. CI runs the same checks.

```bash
make lint           # ruff lint + format check
make typecheck      # mypy
make test           # offline test suite
make test-all       # also runs the Spark parity test (requires Java 17 or 21)
```

Tests must not hit the network. Use the `synthetic` provider, or register a stub provider (see
`tests/conftest.py`). Mark any test that needs a live provider with `@pytest.mark.network`.

To check changes to the Airflow stack:

```bash
docker compose config --quiet
docker compose up -d && docker compose ps
```

## Guidelines

- **Keep orchestration thin.** Logic belongs in `src/finsight`. The CLI, UI, and DAGs only
  translate user input into library calls.
- **Don't overclaim.** A provider declares exactly what it supports in `capabilities`, and docs
  describe what exists today.
- **Be explicit at boundaries.** Providers raise the documented exceptions. Datasets define
  schemas, keys, and checks up front.
- **Test behaviour, not implementation.** Pipeline tests assert on manifests and stored data.
- Follow the existing style. Ruff enforces formatting and imports.

## Adding a provider or dataset

See [docs/providers.md](docs/providers.md). New providers need:

- accurate `capabilities`, including history limits
- error translation into FinSight's exception types
- mocked tests (see `tests/test_yahoo.py`)
- a row in the providers table in the README and `docs/providers.md`

## Commit messages and pull requests

- Use the imperative mood: "Add dividends dataset", not "Added dividends".
- Keep each pull request focused on one change, and describe what changed and how you tested it.
- Update `CHANGELOG.md` under **Unreleased** for user-visible changes.

## Reporting issues

Include your FinSight version (`finsight --version`), the command or code you ran, the run
manifest if relevant (`finsight runs show latest --json`), and what you expected to happen.

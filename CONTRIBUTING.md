This project welcomes contributions. All Pull Requests should include proper testing, documentation, and follow all
existing checks and practices.

<!-- TOC -->
* [Development Workflow](#development-workflow)
  * [Versioning](#versioning)
  * [Development](#development)
    * [Pre-Commit](#pre-commit)
    * [Installing Locally](#installing-locally)
  * [Linting](#linting)
  * [Testing](#testing)
<!-- TOC -->

# Development Workflow

1. Create a branch off `main`
2. Develop, add tests, ensure all tests are passing
3. Push up to GitHub (running pre-commit)
4. Create a PR, get approval
5. Merge the PR to `main`
6. On `main`: Create a tag with `just tag`
7. Do any manual or integration testing
8. Push the tag to GitHub `just deploy-tag`, which will create
   a `Draft` [release](https://github.com/astronomer/orbiter/releases) and upload
   to [test.pypi.org](https://test.pypi.org/project/astronomer-orbiter/) via CICD
9. Approve the [release](https://github.com/astronomer/orbiter/releases) on GitHub, which
   will upload to [pypi.org](https://pypi.org/project/astronomer-orbiter/) via CICD

## Versioning

- This project follows [Semantic Versioning](https://semver.org/)

## Development

### Just

This project uses [`just` (link)](https://just.systems/)

### Pre-Commit

This project uses [`pre-commit` (link)](https://pre-commit.com/).

Install it with `just install-precommit`

### Installing Locally

Install with `just install`

## Linting

This project
uses [`black` (link)](https://black.readthedocs.io/en/stable/), and [`ruff` (link)](https://beta.ruff.rs/).
They run with pre-commit but you can run them directly with `just lint` in the root.

## Testing

This project utilizes [Doctests](https://docs.python.org/3/library/doctest.html) and `pytest`.
With the `dev` extras installed, you can run all tests with `just test` in the root of the project.
It will automatically pick up it's configuration from `pyproject.toml`

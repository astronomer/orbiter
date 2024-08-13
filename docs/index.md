<!--suppress HtmlDeprecatedAttribute -->
<p align="center">
  <img
    width="200px" height="200px"
    src="./orbiter.svg"
    alt="Logo of Spaceship Orbiting a Planet"
  />
</p>
<p align="center">
  <b>Astronomer Orbiter</b> can land legacy workloads safely down in a new home on Apache Airflow!
</p>

## What is Orbiter?
Orbiter is both a CLI and Framework for converting workflows
from other orchestration tools to Apache Airflow.

Generally it can be thoughts of as:
```mermaid
flowchart LR
    origin{{ XML/JSON/YAML/Etc Workflows }}
    origin -->| ✨ Translations ✨ | airflow{{ Apache Airflow Project }}
```
The Orbiter framework is a set of [Rules](./Rules_and_Rulesets) and [Objects](./Objects) that can translate workflows
from an [Origin](./Origins) system to Airflow-native python code.

## Installation
```shell
pip install astronomer-orbiter
```

## Usage
### CLI
```shell
orbiter translate <INPUT_DIR> <OUTPUT_DIR> -r <RULESET>
```

You can see more specifics on how to use the Orbiter CLI in the [CLI](CLI) section.

### Supported Origins
The list of systems Orbiter has support for translating is listed at [Origins](origins)

## Customization
Orbiter can be extended to fit specific needs, patterns, or get support for additional origins or specific translations.

Read more specifics about how to use the framework at [Rules](./Rules_and_Rulesets) and [Objects](./Objects)

### Example

1. Set up a folder like:
    ```shell
    .
    ├── override.py
    └── workflow
        └── my-sample-workflow.json
    ```
2. With contents like:
    ```python title="override.py"
    -8<- "tests/resources/override/override.py"
    ```
3. Invoke with your customization:
    ```shell
    orbiter translate workflow output -r override.translation_ruleset
    ```

## FAQ
- **Can this tool convert my workflows from tool X to Airflow?**

    _If you don't see your tool listed in [Supported Origins](./Origins),
    you can create an [issue](https://github.com/astronomer/orbiter-community-translations/issues/new/)
    to request support for it.
    We also happily accept [pull requests](https://github.com/astronomer/orbiter-community-translations/pulls/) ._

- **Are the results of this tool under any guarantee of correctness?**

    _**No.** This tool is provided as-is, with no guarantee of correctness.
    It is your responsibility to verify the results.
    We accept Pull Requests to improve parsing,
    and strive to make the tool easily configurable to handle your specific use-case._

---

**Artwork**
Orbiter logo [by Ivan Colic](https://thenounproject.com/Ivanisawesome/) used with permission
from [The Noun Project](https://thenounproject.com/icon/lunar-orbiter-196219/)
under [Creative Commons](https://creativecommons.org/licenses/by/3.0/us/legalcode).

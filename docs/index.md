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
Orbiter is both a **CLI** and **Framework** for converting workflows
from other orchestration tools to Apache Airflow.

Generally it can be thoughts of as:
```mermaid
flowchart LR
    origin{{ XML/JSON/YAML/Etc Workflows }}
    origin -->| ✨ Translations ✨ | airflow{{ Apache Airflow Project }}
```
The Orbiter **framework** is a set of [Rules](./Rules_and_Rulesets) and [Objects](./objects) that can translate workflows
from an [Origin](./origins) system to Airflow-native python code.

## Usage - Translate via Translations Rulesets
You can utilize the `orbiter` CLI with pre-built translations to convert workflows from other systems to Apache Airflow.

The list of systems Orbiter has support for translating is listed at [Origins](origins)

### Installation

You can install the Open-Source `orbiter` CLI, if you have Python >= 3.10 installed via `pip`:
```shell
pip install astronomer-orbiter
```
If you do not have a compatible Python environment,
pre-built binary executables of the `orbiter` CLI
are available for download on the [Releases](https://github.com/astronomer/orbiter/releases) page.

### Translate
Use the `orbiter translate` command to convert workflows via a specific translation ruleset

1. Determine the specific translation ruleset via the [Origins](origins) page,
    or [create a translation ruleset](#usage-authoring-rulesets-customizing-translations),
    if one does not exist
2. Set up a new folder, and create a `workflow/` folder. Add your workflows files to it
    ```shell
    .
    └── workflow/
        ├── workflow_a.json
        ├── workflow_b.json
        └── ...
    ```
3. Invoke the `orbiter` CLI (replacing `<RULESET>` with your desired ruleset). This will produce output to an `output/` folder:
    ```shell
    orbiter translate workflow/ output/ --ruleset <RULESET>
    ```
4. Review the contents of the `output/` folder. If extensions or customizations are required, review
    [how to extend a translation ruleset](#extension-or-customization)
5. Utilize the [`astro` CLI](https://www.astronomer.io/docs/astro/cli/overview)
    to run Airflow instance with your migrated workloads
6. Deploy to [Astro](https://www.astronomer.io/try-astro/) to run your translated workflows in production! 🚀

You can see more specifics on how to use the Orbiter CLI in the [CLI](CLI) section.

### Extension or Customization

To extend or customize an existing ruleset, you can easily modify it with simple Python code.

1. Set up your workspace as described in steps 1+2 of the ["Translate"](#translate) instructions
2. Create a Python script, named `override.py`
    ```shell
    .
    ├── override.py
    └── workflow/
        ├── workflow_a.json
        ├── workflow_b.json
        └── ...
    ```
3. Add something like the following to `override.py`:
    ```python title="override.py" linenums="1"
    -8<- "tests/resources/override/override.py"
    ```
    1. Importing specific translation ruleset, determined via the [Origins](origins) page
    2. Importing required [Orbiter Objects](./objects)
    3. Importing required [Rule](./Rules_and_Rulesets) types
    4. Create one or more `@rule` functions, as required. A higher priority means this rule will be applied first.
        [`@task_rule` Reference](./Rules_and_Rulesets/rules/#orbiter.rules.TaskRule)
    5. `Rules` have an `if/else` statement - they must always return a **single** thing or **nothing**
    6. [`OrbiterSSHOperator` Reference](./objects/tasks/#orbiter.objects.operators.ssh.OrbiterSSHOperator)
    7. Append the new [Rule](./Rules_and_Rulesets)
       to the [`translation_ruleset`](./Rules_and_Rulesets/rulesets/#orbiter.rules.rulesets.TranslationRuleset)

4. Invoke the `orbiter` CLI, pointing it at your customized ruleset, and writing output to an `output/` folder:
    ```shell
    orbiter translate workflow output --ruleset override.translation_ruleset
    ```
4. Follow the remaining steps 4 -> 6 of the ["Translate"](#translate) instructions

## Usage - Authoring Rulesets & Customizing Translations
Orbiter can be extended to fit specific needs, patterns, or to support additional origins.

Read more specifics about how to use the framework at [Rules](./Rules_and_Rulesets) and [Objects](./objects)

## FAQ
- **Can this tool convert my workflows from tool X to Airflow?**

    _If you don't see your tool listed in [Supported Origins](./origins),
    [contact us](https://www.astronomer.io/contact/) for services to create translations,
    create an [issue](https://github.com/astronomer/orbiter-community-translations/issues/new/)
    in our `orbiter-community-translations` repository
    write a `TranslationRuleset` and submit a
    [pull request](https://github.com/astronomer/orbiter-community-translations/pulls/)
    to share your translations with the community._

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

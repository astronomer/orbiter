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
The **framework** is a set of [Rules](./Rules_and_Rulesets/index.md) and [Objects](./objects/index.md) that can translate workflows
from an [Origin](./origins.md) system to an Airflow project.

## Installation

Install the [`orbiter` CLI](./cli.md) via `pip` (requires Python >= 3.10):
```shell
pip install astronomer-orbiter
```
If you do not have a compatible Python environment, pre-built binary executables of the `orbiter` CLI
are available for download on the [Releases](https://github.com/astronomer/orbiter/releases) page.

## Translate
Utilize the [`orbiter` CLI](./cli.md) with existing translations to convert workflows
from other systems to an Airflow project.

1. **Set up a new  `workflow/` folder. Add your workflows files to it**
    ```shell
    .
    └── workflow/
        ├── workflow_a.json
        ├── workflow_b.json
        └── ...
    ```
2. Determine the specific translation ruleset and repository via:
    1. the [Origins](./origins.md) documentation
    2. the [`orbiter list-rulesets`](./cli.md#list-rulesets) command
    3. or [by creating a translation ruleset](#authoring-rulesets-customization), if one does not exist
3. **Install the translation ruleset** via the [`orbiter install`](./cli.md#install) command
4. **Translate workloads** via the [`orbiter translate`](./cli.md#translate) command
5. **Review** the contents of the output folder (default: `output/`).
  If extensions or customizations are required, review [how to extend a translation ruleset](#extend-or-customize)
6. (optional) **Initialize** a full Airflow project
   containing your migrated workloads with the [`astro` CLI](https://www.astronomer.io/docs/astro/cli/overview)
7. (optional) **Deploy** to [Astro](https://www.astronomer.io/try-astro/) to run your translated workflows in production! 🚀

## Authoring Rulesets & Customization
Orbiter can be extended to fit specific needs, patterns, or to support additional origins.

Read more specifics about how to use the framework at [Rules](./Rules_and_Rulesets/index.md) and [Objects](./objects/index.md)

### Extend or Customize
To extend or customize an existing ruleset, you can easily modify it with simple Python code.

1. Set up your workspace as described in steps 1+2 of the [Translate](#translate) instructions
2. Create a Python script, named `override.py`
    ```shell
    .
    ├── override.py
    └── workflow/
        ├── workflow_a.json
        ├── workflow_b.json
        └── ...
    ```
3. Add contents to `override.py`:
    ```python title="override.py" linenums="1"
    -8<- "tests/resources/override/override.py"
    ```
    1. Importing specific translation ruleset, determined via the [Origins](origins.md) page
    2. Importing required [Objects](./objects/index.md)
    3. Importing required [Rule](./Rules_and_Rulesets/index.md) types
    4. Create one or more `@rule` functions, as required. A higher priority means this rule will be applied first.
        [`@task_rule` Reference](./Rules_and_Rulesets/rules.md#orbiter.rules.TaskRule)
    5. `Rules` have an `if/else` statement - they must always return a **single** thing or **nothing**
    6. [`OrbiterSSHOperator` Reference](./objects/Tasks/Operators.md#orbiter.objects.operators.ssh.OrbiterSSHOperator)
    7. Append the new [Rule](./Rules_and_Rulesets/index.md)
       to the [`translation_ruleset`](./Rules_and_Rulesets/rulesets.md#orbiter.rules.rulesets.TranslationRuleset)

4. Invoke the [`orbiter translate`](./cli.md#translate) command, pointing it at your customized ruleset
    ```shell
    orbiter translate --ruleset override.translation_ruleset
    ```
5. Follow the remaining steps of the [Translate](#translate) instructions

### Authoring a new Ruleset

You can utilize the [`TranslationRuleset` Template](./Rules_and_Rulesets/template.md)
to create a new [`TranslationRuleset`][orbiter.rules.rulesets.TranslationRuleset].

## FAQ
- **Can this tool convert my workflows from tool X to Airflow?**

    _If you don't see your tool listed in [Supported Origins](./origins.md),
    [contact us](https://www.astronomer.io/contact/) for services to create translations,
    create an [issue](https://github.com/astronomer/orbiter-community-translations/issues/new/)
    in the [`orbiter-community-translations`](https://github.com/astronomer/orbiter-community-translations) repository, or write a [`TranslationRuleset`](./Rules_and_Rulesets/template.md) and submit a
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

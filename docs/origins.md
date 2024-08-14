
An Origin is a source system that contains workflows that can be translated to an Apache Airflow project.

## Supported Origins

| Origin      | Maintainer                                                                | Ruleset(s)                                   |
|-------------|---------------------------------------------------------------------------|----------------------------------------------|
| DAG Factory | [Community](https://github.com/astronomer/orbiter-community-translations) | `orbiter_translations.dag_factory.yaml_base` |
| Control M   | Astronomer                                                                | `orbiter_translations.control_m.json_base`   |
|             |                                                                           | `orbiter_translations.control_m.json_ssh`    |
|             |                                                                           | `orbiter_translations.control_m.xml_base`    |
|             |                                                                           | `orbiter_translations.control_m.xml_ssh`     |
| Automic     | Astronomer                                                                |                                              |
| Autosys     | Astronomer                                                                |                                              |
| JAMS        | Astronomer                                                                |                                              |
| SSIS        | Astronomer                                                                | `orbiter_translations.ssis.xml_base`         |
| Oozie       | Astronomer                                                                | `orbiter_translations.oozie.xml_base`        |
| **& more!** |                                                                           |                                              |

For [Astronomer](https://www.astronomer.io) maintained Translation Rulesets,
please [contact us](https://www.astronomer.io/contact/) for access to the most up-to-date versions.

If you don't see your Origin system listed, please either:

- [contact us](https://www.astronomer.io/contact/) for services to create translations
- create an [issue](https://github.com/astronomer/orbiter-community-translations/issues/new/) in our `orbiter-community-translations` repository
- write a `TranslationRuleset` and submit a [pull request](https://github.com/astronomer/orbiter-community-translations/pulls/) to share your translations with the community

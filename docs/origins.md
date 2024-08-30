An Origin is a source system that contains workflows that can be translated to an Apache Airflow project.

## Supported Origins

```python exec="on"
from csv import DictReader
from tabulate import tabulate
import pkgutil

print(tabulate(
    list(DictReader(pkgutil.get_data('orbiter.assets', 'supported_origins.csv').decode().splitlines())),
    headers="keys",
    tablefmt="pipe"
))
```

For [Astronomer](https://www.astronomer.io) maintained Translation Rulesets,
please [contact us](https://www.astronomer.io/contact/) for access to the most up-to-date versions.

If you don't see your Origin system listed, please either:

- [contact us](https://www.astronomer.io/contact/) for services to create translations
- create an [issue](https://github.com/astronomer/orbiter-community-translations/issues/new/) in our [`orbiter-community-translations`](https://github.com/astronomer/orbiter-community-translations) repository
- write a [`TranslationRuleset` Template](./Rules_and_Rulesets/template.md) and submit a [pull request](https://github.com/astronomer/orbiter-community-translations/pulls/) to share your translations with the community

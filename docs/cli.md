# CLI

::: mkdocs-click
    :module: orbiter.__main__
    :command: orbiter
    :depth: 1
    :style: table

## Logging

You can alter the verbosity of the CLI by setting the `LOG_LEVEL` environment variable. The default is `INFO`.

```shell
export LOG_LEVEL=DEBUG
```


## Autocomplete

Auto-completion is available for the CLI. To enable it, run the following command or place it in `.bashrc` or `.zshrc`:

### Bash
```shell
eval "$(_ORBITER_COMPLETE=bash_source orbiter)"
```


### ZSH
```shell
eval "$(_ORBITER_COMPLETE=zsh_source orbiter)"
```

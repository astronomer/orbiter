from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger
from pydantic import validate_call

from orbiter import trim_dict
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.project import OrbiterProject
from orbiter.objects.task import OrbiterOperator, OrbiterTaskDependency

if TYPE_CHECKING:
    from orbiter.rules.rulesets import TranslationRuleset
    from orbiter.objects.task_group import OrbiterTaskGroup


def validate_translate_function_inputs(translation_ruleset: TranslationRuleset, input_dir: Path) -> None:
    """Validate the inputs to the translation function

    !!! note

        ```pycon
        >>> from orbiter.rules.rulesets import EMPTY_TRANSLATION_RULESET
        >>> validate_translate_function_inputs(None, Path())  # noqa
        Traceback (most recent call last):
        TypeError: Error! type(translation_ruleset)==<class 'NoneType'>!=TranslationRuleset! Exiting!
        >>> validate_translate_function_inputs(EMPTY_TRANSLATION_RULESET, '.')  # noqa
        Traceback (most recent call last):
        TypeError: Error! type(input_dir)==<class 'str'>!=Path! Exiting!

        ```

    :raises TypeError: if input is an invalid type
    """
    from orbiter.rules.rulesets import TranslationRuleset

    if not isinstance(translation_ruleset, TranslationRuleset):
        raise TypeError(f"Error! type(translation_ruleset)=={type(translation_ruleset)}!=TranslationRuleset! Exiting!")
    if not isinstance(input_dir, Path):
        raise TypeError(f"Error! type(input_dir)=={type(input_dir)}!=Path! Exiting!")


def file_relative_to_parent(file: Path, parent: Path) -> Path:
    """Get the file relative to the parent, if possible, otherwise return the absolute path

    The returned value *includes the parent*, but nothing before it.

    ```pycon
    >>> file_relative_to_parent(Path("/a/b/c/d.txt"), Path("/a/b"))
    PosixPath('b/c/d.txt')
    >>> file_relative_to_parent(Path("/a/b/c/d.txt"), Path("/g"))
    PosixPath('a/b/c/d.txt')

    ```
    """
    file_resolved = file.resolve()
    try:
        file_relative_to_input_dir_parent = file_resolved.relative_to(parent.parent.resolve())
    except ValueError as e:
        logger.opt(exception=e).warning(f"File {file_resolved} is not relative to {parent.parent}")
        file_relative_to_input_dir_parent = file_resolved
    return file_relative_to_input_dir_parent


@dataclass
class CandidateInput:
    i: int
    input_dict: dict
    file: Path
    input_dir: Path
    translation_ruleset: TranslationRuleset


# noinspection t,D
@validate_call
def translate(translation_ruleset, input_dir: Path) -> OrbiterProject:
    """
    Orbiter expects a folder containing text files which may have a structure like:
    ```json
    {"<workflow name>": { ...<workflow properties>, "<task name>": { ...<task properties>} }}
    ```
    However, the files may be in any format, and the structure may be different.

    The standard translation function performs the following steps:

    ![Diagram of Orbiter Translation](../orbiter_diagram.png)

    1. [**Find all files**][orbiter.rules.rulesets.TranslationRuleset.get_files_with_extension] with the expected
        [`TranslationRuleset.file_type`][orbiter.rules.rulesets.TranslationRuleset]
        (`.json`, `.xml`, `.yaml`, etc.) in the input folder.
        - [**Load each file**][orbiter.rules.rulesets.TranslationRuleset.loads] and turn it into a Python Dictionary.
    2. **For each file:** Apply the [`TranslationRuleset.dag_filter_ruleset`][orbiter.rules.rulesets.DAGFilterRuleset]
        to filter down to entries that can translate to a DAG, in priority order.
        - **For each dictionary**: Apply the [`TranslationRuleset.dag_ruleset`][orbiter.rules.rulesets.DAGRuleset],
        to convert the object to an [`OrbiterDAG`][orbiter.objects.dag.OrbiterDAG],
        in priority-order, stopping when the first rule returns a match.
        If no rule returns a match, the entry is filtered.
    3. Apply the [`TranslationRuleset.task_filter_ruleset`][orbiter.rules.rulesets.TaskFilterRuleset]
        to filter down to entries in the DAG that can translate to a Task, in priority-order.
        - **For each:** Apply the [`TranslationRuleset.task_ruleset`][orbiter.rules.rulesets.TaskRuleset],
            to convert the object to a specific Task, in priority-order, stopping when the first rule returns a match.
            If no rule returns a match, the entry is filtered.
    4. After the DAG and Tasks are mapped, the
        [`TranslationRuleset.task_dependency_ruleset`][orbiter.rules.rulesets.TaskDependencyRuleset]
        is applied in priority-order, stopping when the first rule returns a match,
        to create a list of
        [`OrbiterTaskDependency`][orbiter.objects.task.OrbiterTaskDependency],
        which are then added to each task in the
        [`OrbiterDAG`][orbiter.objects.dag.OrbiterDAG]
    5. Apply the [`TranslationRuleset.post_processing_ruleset`][orbiter.rules.rulesets.PostProcessingRuleset],
        against the [`OrbiterProject`][orbiter.objects.project.OrbiterProject], which can make modifications after all
        other rules have been applied.
    6. After translation - the [`OrbiterProject`][orbiter.objects.project.OrbiterProject]
        is rendered to the output folder.

    !!! info

        Two modifications are made to both input and output dictionaries for a
        [`@dag_filter_rule`][orbiter.rules.DAGFilterRule]:

        - the initial input directory is added under a `__input_dir` key to each dictionary
        - the relative file name is added under a `__file` key

    !!! info
        Information about matching rules is saved in an `orbiter_meta` property on each resulting orbiter object, via
        an [`OrbiterMeta`][orbiter.meta.OrbiterMeta] object.

    !!! info
        Input is saved via an `orbiter_kwargs` property on each object. This can be useful for later
        [`OrbiterTaskDependency`][orbiter.objects.task.OrbiterTaskDependency] or
        [`TranslationRuleset.post_processing_ruleset`][orbiter.rules.rulesets.PostProcessingRuleset] modifications.
    """
    validate_translate_function_inputs(translation_ruleset, input_dir)

    logger.debug("Creating an empty OrbiterProject")
    project = OrbiterProject()

    logger.info("Finding all files with the expected file type")
    files: enumerate[tuple[Path, dict]] = enumerate(translation_ruleset.get_files_with_extension(input_dir))

    def filter_dags(i: int, file: Path, input_dict: dict) -> tuple[list[dict], str]:
        """Step 1) Filter Dags from a file"""
        file_relative_to_input_dir_parent = file_relative_to_parent(file, input_dir)
        file_log_prefix = f"[File {i}={file_relative_to_input_dir_parent}]"
        logger.info(f"{file_log_prefix} Translating file")

        logger.debug(f"{file_log_prefix} Extracting DAG candidates")
        dag_dicts: list[dict] = translation_ruleset.dag_filter_ruleset.apply_ruleset(
            input_dict=input_dict, file=file_relative_to_input_dir_parent, input_dir=input_dir
        )
        logger.debug(f"{file_log_prefix} Found {len(dag_dicts)} DAG candidates")
        return dag_dicts, file_log_prefix

    def extract_dag(dag_dict: dict, file_log_prefix: str) -> tuple[OrbiterDAG | None, str]:
        """Step 2) Extract Dag from filtered Dag"""
        logger.debug(f"{file_log_prefix} Translating DAG Candidate to DAG")
        dag: OrbiterDAG = translation_ruleset.dag_ruleset.apply_ruleset(dag_dict=dag_dict)
        if dag:
            dag_log_prefix = f"{file_log_prefix}[DAG={dag.dag_id}]"
            return dag, dag_log_prefix
        else:
            return None, file_log_prefix

    def filter_tasks(
        dag_dict: dict,
        dag_log_prefix: str,
    ) -> list[dict]:
        """Step 3) Filter tasks from a filtered Dag"""
        logger.debug(f"{dag_log_prefix} Extracting Task Candidates to Tasks")
        task_dicts: list[dict] = translation_ruleset.task_filter_ruleset.apply_ruleset(dag_dict=dag_dict)
        logger.debug(f"{dag_log_prefix} Found {len(task_dicts)} Task candidates")
        return task_dicts

    def extract_tasks(task_dicts: list[dict], dag_log_prefix: str) -> list[OrbiterOperator | OrbiterTaskGroup]:
        logger.debug(f"{dag_log_prefix} Translating Task Candidates to Tasks")
        tasks = []
        for task_dict in task_dicts:
            task: OrbiterOperator | OrbiterTaskGroup = translation_ruleset.task_ruleset.apply_ruleset(
                task_dict=task_dict
            )
            if task is None:
                logger.warning(f"{dag_log_prefix} Couldn't extract task from expected task_dict={task_dict}")
                continue
            tasks.append(task)
        logger.debug(f"{dag_log_prefix} Adding {len(tasks)} tasks")
        return tasks

    def extract_task_dependencies(dag: OrbiterDAG, dag_log_prefix: str) -> None:
        """Step 4) Extract task dependencies from a filtered dag, add in-place"""
        logger.debug(f"{dag_log_prefix} Extracting Task Dependencies to apply to Tasks")
        task_dependencies: list[OrbiterTaskDependency] = translation_ruleset.task_dependency_ruleset.apply_ruleset(
            dag=dag
        )
        if not len(task_dependencies):
            logger.warning(f"{dag_log_prefix} Couldn't find task dependencies in dag={trim_dict(dag.orbiter_kwargs)}")

        logger.debug(f"{dag_log_prefix} Adding Task Dependencies to Tasks")
        for task_dependency in task_dependencies:
            if parent := dag.get_task_dependency_parent(task_dependency):
                parent.tasks[task_dependency.task_id].add_downstream(task_dependency)
            else:
                logger.warning(f"{dag_log_prefix} Couldn't find task_id={task_dependency.task_id} in tasks")
                continue

    # Translate each file individually - Default
    if not getattr(translation_ruleset.config, "upfront", False):
        for i, (file, input_dict) in files:
            dag_dicts, file_log_prefix = filter_dags(i, file, input_dict)
            for dag_dict in dag_dicts:
                dag, dag_log_prefix = extract_dag(dag_dict, file_log_prefix)
                if dag is None:
                    logger.warning(f"{file_log_prefix} Couldn't extract DAG from dag_dict={dag_dict}")
                    continue

                dag.add_tasks(extract_tasks(filter_tasks(dag_dict, dag_log_prefix), dag_log_prefix))
                extract_task_dependencies(dag, dag_log_prefix)

                logger.debug(f"{dag_log_prefix} Adding DAG {dag.dag_id} to project")
                project.add_dags(dag)

    # Filter against all files upfront
    else:
        import itertools

        def filter_dag_candidates_in_file(candidate_input: CandidateInput) -> list[tuple[str, dict, list[dict]]]:
            """Filter Dag and Task candidates from a file, upfront"""
            dag_dicts, file_log_prefix = filter_dags(
                candidate_input.i, candidate_input.file, candidate_input.input_dict
            )
            return [
                (file_log_prefix, dag_dict, filter_tasks(dag_dict, f"{file_log_prefix}[DAG=???]"))
                for dag_dict in dag_dicts
            ]

        if getattr(translation_ruleset.config, "parallel", False):
            import sys
            import pathos

            logger.info("Filtering DAG and Task Candidates from all files in parallel")
            with (  # noqa
                # pyinstaller/exe can't do multiproc
                pathos.multiprocessing.ThreadPool
                if getattr(sys, "frozen", False)
                else pathos.multiprocessing.ProcessingPool
            )() as pool:
                dag_and_task_candidates_all_files = pool.map(
                    filter_dag_candidates_in_file,
                    (
                        CandidateInput(i, input_dict, file, input_dir, translation_ruleset)
                        for i, (file, input_dict) in files
                    ),
                )
        else:
            logger.info("Filtering DAG and Task Candidates from all files in serial")
            dag_and_task_candidates_all_files = map(
                filter_dag_candidates_in_file,
                (
                    CandidateInput(i, input_dict, file, input_dir, translation_ruleset)
                    for i, (file, input_dict) in files
                ),
            )

        logger.info("Extracting Dags and Tasks in serial")
        for file_log_prefix, dag_dict, task_dicts in itertools.chain(*dag_and_task_candidates_all_files):
            dag, dag_log_prefix = extract_dag(dag_dict, file_log_prefix)
            if dag is None:
                logger.warning(f"{file_log_prefix} Couldn't extract DAG from dag_dict={dag_dict}")
                continue
            dag.add_tasks(extract_tasks(task_dicts, dag_log_prefix))
            extract_task_dependencies(dag, dag_log_prefix)

            logger.debug(f"{dag_log_prefix} Adding DAG {dag.dag_id} to project")
            project.add_dags(dag)

    logger.debug("Applying post-processing ruleset")
    translation_ruleset.post_processing_ruleset.apply_ruleset(project=project)

    return project


def fake_translate(translation_ruleset: TranslationRuleset, input_dir: Path) -> OrbiterProject:
    """Fake translate function, for testing"""
    _ = (translation_ruleset, input_dir)
    return OrbiterProject()

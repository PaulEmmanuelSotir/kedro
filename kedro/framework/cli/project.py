# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""A collection of CLI commands for working with Kedro project."""

import os
import json
import shutil
import subprocess
import sys
import webbrowser
from pathlib import Path
from typing import Any, Sequence, Union, List

import click
from click import secho

import anyconfig

from kedro.framework.cli.cli import _handle_exception
from kedro.framework.cli.utils import (
    KedroCliError,
    _check_module_importable,
    call,
    env_option,
    forward_command,
    get_source_dir,
    ipython_message,
    python_call,
)
from kedro.framework.context import load_context

NO_DEPENDENCY_MESSAGE = """{module} is not installed. Please make sure {module} is in
{src}/requirements.txt and run `kedro install`."""
LINT_CHECK_ONLY_HELP = """Check the files for style guide violations, unsorted /
unformatted imports, and unblackened Python code without modifying the files."""
OPEN_ARG_HELP = """Open the documentation in your default browser after building."""
CONDA_ARG_HELP = f""" Conda environement filename. Optionnal argument (str or List[str]) specifying conda
environement configuration filename(s) located in source directory. Default value: "['environment.yml', 'environment.yaml']",
which means 'kedro install' command will create/update conda environement using the first conda environement file whose name
matches, if any. Conda environement is created under the prefix/directory "{CONDA_PREFIX}" (or user defined prefix in environment 
file if any), except if you already created a conda environement manually before calling 'kedro install', then conda environement 
will only be updated. Conda environement name is parsed from environement file if specified, otherwise conda environment will be 
named after project name. Kedro install command will try to find existing conda environement to update according to YML env file's 
'prefix' and/or 'name' before creating a new one. """

def _load_project_context(**kwargs: Any):
    """Returns project context."""
    try:
        return load_context(Path.cwd(), **kwargs)
    except Exception as err:  # pylint: disable=broad-except
        env = kwargs.get("env")
        _handle_exception(
            f"Unable to load Kedro context with environment `{env}`. "
            f"Make sure it exists in the project configuration.\nError: {err}"
        )


def _build_reqs(source_path: Path, args: Sequence[str] = ()):
    """Run `pip-compile requirements.in` command.

    Args:
        source_path: Path to the project `src` folder.
        args: Optional arguments for `pip-compile` call, e.g. `--generate-hashes`.

    """
    requirements_in = source_path / "requirements.in"

    if not requirements_in.is_file():
        secho("No requirements.in found. Copying contents from requirements.txt...")
        requirements_txt = source_path / "requirements.txt"
        shutil.copyfile(str(requirements_txt), str(requirements_in))

    python_call("piptools", ["compile", "-q", *args, str(requirements_in)])


def _run_conda_cmd(*args, **run_kwargs):
    """ Added for better conda environment support (update or intsall throught `kedro install` command) """
    proc = subprocess.run(['conda', *args, '--json', '-q'], **run_kwargs)
    return proc.returncode, json.loads(proc.stdout)


def _conda_activate():
    """ Added for better conda environment support (update or intsall throught `kedro install` command) """
    raise NotImplementedError  # TODO: implement this
    # call(['source', 'activate', conda_prefix] if 'posix' in os.name else ['activate', conda_prefix])


def _ask(prompt: str, choices: List = ['N', 'Y'], ask_indexes: bool = False):
    """ Helper function used to ask user to answer or choose among multiple alternatives using a CLI prompt """
    prompt += ' (Choices: ' + ('; '.join([f'{i}: "{str(e)}""' for i, e in enumerate(choices)]) if ask_indexes else '; '.join(map(str, choices)))
    choice = input(prompt)

    if ask_indexes:
        while not choice.isdigit() or int(choice) not in range(len(choices)):
            choice = input(prompt)
        return int(choice), choices[int(choice)]
    else:
        while(choice not in choices):
            choice = input(prompt)
        return choices.index(choice), choice


def _conda_install(conda_yml: Union[str, List[str]]):
    """ Added for better conda environment support (update or intsall throught `kedro install` command) """
    for conda_config in conda_yml if isinstance(conda_yml, List) else [conda_yml]:
        config_path = Path.cwd() / get_source_dir(Path.cwd()) / conda_config
        if config_path.is_file():
            # Pasre existing conda environement file
            secho(f'Found existing conda configuration file: "{config_path}".')
            conda_cfg = anyconfig.load(config_path, ac_parser='yaml')
            if not conda_cfg:
                secho(f'Error: Can\'t parse or empty conda environment file "{config_path}"', fg='red')
                sys.exit(1)
            cfg_name, cfg_prefix = conda_cfg.get('name'), conda_cfg.get('prefix')
            name = cfg_name if cfg_name else PROJ_NAME
            prefix = cfg_prefix if cfg_prefix else DEFAULT_CONDA_PREFIX

            returncode, json = _run_conda_cmd('env', 'list')
            if returncode:
                secho('Error: Failed to list conda environments, Conda may not be installed.', fg='red')
                sys.exit(1)

            # Find out conda environement name and prefix
            update = False
            matching_envs = [Path(env) for env in json['envs'] if os.path.split(env)[1] == cfg_name]
            if ((cfg_prefix and cfg_name and Path(str(cfg_prefix)) / cfg_name in json['envs']) or (not cfg_name and cfg_prefix and Path(str(cfg_prefix)) in json['envs'])):
                secho(f'Found existing conda environment with matching prefix ("{cfg_prefix}") and name ("{cfg_name}").')
                update = True
            elif any(matching_envs) and not conda_cfg:
                choice = matching_envs[0]
                if len(matching_envs) == 1:
                    secho(f'Found a conda environment with matching name ("{matching_envs[0]}") but no prefix have been specified in env file.', fg='yellow')
                    _, update = _ask('Reuse existing conda environement? (Y/N)')
                else:
                    secho(f'Found multiple conda environments with matching name ("{matching_envs}") and no prefix have been specified in env file.', fg='yellow')
                    _, update = _ask('Would you reuse one of existing conda environements or create a new one? (Y/N)')
                    if update:
                        prompt = f'Which conda environement would you reuse?'
                        _, choice = _ask(prompt, choices=matching_envs, ask_indexes=True)
                if update:
                    prefix, name = os.path.split(choice)

            # Create or update conda environement file
            if update:
                secho('Trying to update conda environement...')
                call(['conda', 'env', 'update', '--prefix', prefix, '--name', name, '--file', str(config_path), '--prune', '--json', '-q'])
                secho('Success.', fg='green')
            else:
                secho('Trying to create conda environement...')
                call(['conda', 'env', 'create', '--prefix', prefix, '--name', name, '--file', str(config_path), '--json', '-q'])
                secho('Success.', fg='green')

            # Update environement file in order to contain environement name and prefix explicity (avoids eventual future user prompts to choose conda environement to reuse or not)
            secho('Updating environement file to include name and prefix')
            if name:
                conda_cfg['name'] = name
            if prefix:
                conda_cfg['prefix'] = prefix
            anyconfig.dump(conda_cfg, ac_parser='yaml', out=config_path)
            secho('Sucessfully updated or installed Conda environment from `Kedro install` command.', fg='green')

            # TODO: make sure to activate conda environement before installing other dependencies from pip requirements.txt??
            #_conda_activate()
            return


@click.group()
def project_group():
    """Collection of project commands."""


@forward_command(project_group, forward_help=True)
def test(args):
    """Run the test suite."""
    try:
        # pylint: disable=import-outside-toplevel,unused-import
        import pytest  # noqa
    except ImportError:
        context = _load_project_context()
        source_path = get_source_dir(context.project_path)
        raise KedroCliError(
            NO_DEPENDENCY_MESSAGE.format(module="pytest", src=str(source_path))
        )
    else:
        python_call("pytest", args)


@project_group.command()
@click.option("-c", "--check-only", is_flag=True, help=LINT_CHECK_ONLY_HELP)
@click.argument("files", type=click.Path(exists=True), nargs=-1)
def lint(files, check_only):
    """Run flake8, isort and (on Python >=3.6) black."""
    context = _load_project_context()
    source_path = get_source_dir(context.project_path)
    files = files or (
        str(source_path / "tests"),
        str(source_path / context.package_name),
    )

    try:
        # pylint: disable=import-outside-toplevel, unused-import
        import flake8  # noqa
        import isort  # noqa
        import black  # noqa
    except ImportError as exc:
        raise KedroCliError(
            NO_DEPENDENCY_MESSAGE.format(module=exc.name, src=str(source_path))
        )

    python_call("black", ("--check",) + files if check_only else files)
    python_call("flake8", ("--max-line-length=88",) + files)

    check_flag = ("-c",) if check_only else ()
    python_call(
        "isort", (*check_flag, "-rc", "-tc", "-up", "-fgw=0", "-m=3", "-w=88") + files
    )


@project_group.command()
@click.option("--build-reqs/--no-build-reqs",
             "compile_flag",
             default=None,
             help="Run `pip-compile` on project requirements before install. By default runs only if `src/requirements.in` file doesn't exist.",)
@click.option('--conda-yml', '-c', 'conda_yml', type=Union[str, List[str]], default=['environment.yml', 'environment.yaml'], multiple=False, help=CONDA_ARG_HELP)
def install(compile_flag, conda_yml: Union[str, List[str]]):
    """Install project dependencies from both requirements.txt
    and environment.yml (optional)."""
    # we cannot use `context.project_path` as in other commands since
    # context instantiation might break due to missing dependencies
    # we attempt to install here
    source_path = get_source_dir(Path.cwd())
    requirements_in = source_path / "requirements.in"
    requirements_txt = source_path / "requirements.txt"

    # TODO: parse json outputs from conda (and eventually use 'conda info --envs --json' in order to see available envs (sucessfull creation))
    if conda_yml:
        _conda_install(conda_yml)

    default_compile = bool(compile_flag is None and not requirements_in.is_file())
    do_compile = compile_flag or default_compile
    if do_compile:
        _build_reqs(source_path)

    pip_command = ["install", "-U", "-r", str(requirements_txt)]

    if os.name == "posix":
        python_call("pip", pip_command)
    else:
        command = [sys.executable, "-m", "pip"] + pip_command
        subprocess.Popen(command, creationflags=subprocess.CREATE_NEW_CONSOLE)
    secho("Requirements installed!", fg="green")


@forward_command(project_group, forward_help=True)
@env_option
def ipython(env, args):
    """Open IPython with project specific variables loaded."""
    context = _load_project_context(env=env)
    _check_module_importable("IPython")
    os.environ["IPYTHONDIR"] = str(context.project_path / ".ipython")
    if env:
        os.environ["KEDRO_ENV"] = env
    if "-h" not in args and "--help" not in args:
        ipython_message()
    call(["ipython"] + list(args))


@project_group.command()
def package():
    """Package the project as a Python egg and wheel."""
    context = _load_project_context()
    source_path = get_source_dir(context.project_path)
    call(
        [sys.executable, "setup.py", "clean", "--all", "bdist_egg"],
        cwd=str(source_path),
    )
    call(
        [sys.executable, "setup.py", "clean", "--all", "bdist_wheel"],
        cwd=str(source_path),
    )


@project_group.command("build-docs")
@click.option(
    "--open",
    "-o",
    "open_docs",
    is_flag=True,
    multiple=False,
    default=False,
    help=OPEN_ARG_HELP,
)
def build_docs(open_docs):
    """Build the project documentation."""
    context = _load_project_context()
    source_path = get_source_dir(context.project_path)
    python_call("pip", ["install", str(source_path / "[docs]")])
    python_call("pip", ["install", "-r", str(source_path / "requirements.txt")])
    python_call("ipykernel", ["install", "--user", f"--name={context.package_name}"])
    shutil.rmtree("docs/build", ignore_errors=True)
    call(
        [
            "sphinx-apidoc",
            "--module-first",
            "-o",
            "docs/source",
            str(source_path / context.package_name),
        ]
    )
    call(["sphinx-build", "-M", "html", "docs/source", "docs/build", "-a"])
    if open_docs:
        docs_page = (Path.cwd() / "docs" / "build" / "html" / "index.html").as_uri()
        secho(f"Opening {docs_page}")
        webbrowser.open(docs_page)


@forward_command(project_group, name="build-reqs")
def build_reqs(args):
    """Build the project dependency requirements."""
    # we cannot use `context.project_path` as in other commands since
    # context instantiation might break due to missing dependencies
    # we attempt to install here
    source_path = get_source_dir(Path.cwd())
    _build_reqs(source_path, args)
    secho(
        "Requirements built! Please update requirements.in "
        "if you'd like to make a change in your project's dependencies, "
        "and re-run build-reqs to generate the new requirements.txt.",
        fg="green",
    )


@project_group.command("activate-nbstripout")
def activate_nbstripout():
    """Install the nbstripout git hook to automatically clean notebooks."""
    context = _load_project_context()
    source_path = get_source_dir(context.project_path)
    secho(
        (
            "Notebook output cells will be automatically cleared before committing"
            " to git."
        ),
        fg="yellow",
    )

    try:
        # pylint: disable=import-outside-toplevel, unused-import
        import nbstripout  # noqa
    except ImportError:
        raise KedroCliError(
            NO_DEPENDENCY_MESSAGE.format(module="nbstripout", src=str(source_path))
        )

    try:
        res = subprocess.run(  # pylint: disable=subprocess-run-check
            ["git", "rev-parse", "--git-dir"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if res.returncode:
            raise KedroCliError("Not a git repository. Run `git init` first.")
    except FileNotFoundError:
        raise KedroCliError("Git executable not found. Install Git first.")

    call(["nbstripout", "--install"])

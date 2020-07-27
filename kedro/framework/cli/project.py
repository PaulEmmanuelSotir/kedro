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
CONDA_ARG_HELP = f""" Conda environement filename. Optional argument (str) specifying conda
environement configuration filename located in source directory. Default value: "['environment.yml', 'environment.yaml', 'env.yml', 'env.yaml']",
which means 'kedro install' command will try to create or update conda environement using the 
first conda environement file whose name matches one of these strings. """

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
    cmd = ['conda', *args, '--json', '-q']
    proc = subprocess.run(['conda', *args, '--json', '-q'], **run_kwargs, capture_output=True)
    if proc.stdout is None:
        secho(f'Error occured when running conda command: can`t retrieve JSON output. (cmd="{" ".join(cmd)}"; exit_status="{proc.returncode}")', fg='red')
        sys.exit(1)
    return proc.returncode, json.loads(proc.stdout)


def _conda_install(conda_yml: Union[str, List[str]]):
    """ Added for better conda environment support (update or intsall throught `kedro install` command) """
    secho('#' * 10 + ' Installing or updating conda environement if needed... ' + '#' * 10)
    for conda_config in conda_yml if isinstance(conda_yml, List) else [conda_yml]:
        config_path = get_source_dir(Path.cwd()) / conda_config
        if config_path.is_file():
            # Pasre existing conda environement file
            secho(f'Found existing conda configuration file: "{config_path}".')
            conda_cfg = anyconfig.load(config_path, ac_parser='yaml')
            if not conda_cfg:
                secho(f'Error: Can\'t parse or empty conda environment file "{config_path}"', fg='red')
                sys.exit(1)
            cfg_name, cfg_prefix = conda_cfg.get('name'), conda_cfg.get('prefix')
            if cfg_name and cfg_prefix:
                secho(f'Error: Conda environment file can\' specify a prefix ({cfg_prefix}) and a name ({cfg_name}) at once.', fg='red')
                sys.exit(1)
            if not cfg_name and not cfg_prefix:
                name = DEFAULT_CONDA_ENV_NAME
            else:
                name = cfg_name if cfg_name else cfg_prefix

            returncode, json = _run_conda_cmd('env', 'list')
            if returncode:
                secho(f'Error: Failed to list conda environments, Conda may not be installed (exit_status="{returncode}").', fg='red')
                sys.exit(1)

            # Find out conda environement name or prefix
            matching_envs = [Path(env) for env in json['envs'] if Path(env).name == name or (Path(name).exists() and Path(env).resolve() == Path(name).resolve())]
            if any(matching_envs):
                # Update conda environement
                secho(f'Found existing conda environment with matching prefix or name ("{matching_envs[0]}").')
                secho('#' * 10 + ' Trying to update conda environement... ' + '#' * 10)
                env_identifier = ('--name', name) if not cfg_prefix else ('--prefix', name)
                call(['conda', 'env', 'update', *env_identifier, '--file', str(config_path), '--prune', '--json', '-q'])
            else:
                # Create conda environement
                secho('#' * 10 + ' Trying to create conda environement... ' + '#' * 10)
                env_identifier = ('--name', name) if not cfg_prefix else ('--prefix', name)  # We assume here that DEFAULT_CONDA_ENV_NAME isn't a prefix
                call(['conda', 'env', 'create', *env_identifier, '--file', str(config_path), '--json', '-q'])

            # Update environement file in order to contain environement name (in case of prefix envionment, prefix can't be missing from evn file as default behavior is to create conda environment without prefix but a name)
            if not cfg_prefix and not cfg_name:
                secho('Updating environement file to include new conda environement name')
                conda_cfg['name'] = name
                anyconfig.dump(conda_cfg, ac_parser='yaml', out=config_path)

            secho('#### Sucessfully updated or installed Conda environment from `Kedro install` command. ###', fg='green')
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

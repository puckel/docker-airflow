#!/usr/bin/env python

# From: https://github.com/jlowin/git-sync

from __future__ import print_function
import click
import datetime
import os
import shlex
import subprocess
import sys
import time
# try to be py2/3 compatible
try:
     from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse

def sh(*args, **kwargs):
    """ Get subprocess output"""
    return subprocess.check_output(*args, **kwargs).decode().strip()

def get_repo_at(dest):
    if not os.path.exists(os.path.join(dest, '.git')):
        raise ValueError('No repo found at {dest}'.format(**locals))

    current_remote = sh(
        shlex.split('git config --get remote.origin.url'),
        cwd=dest)

    current_branch = sh(
            shlex.split('git rev-parse --abbrev-ref HEAD'),
            cwd=dest)

    return current_remote.lower(), current_branch.lower()

def setup_repo(repo, dest, branch):
    """
    Clones `branch` of remote `repo` to `dest`, if it doesn't exist already.
    Raises an error if a different repo or branch is found.
    """
    dest = os.path.expanduser(dest)

    repo_name = urlparse(repo).path

    # if no git repo exists at dest, clone the requested repo
    if not os.path.exists(os.path.join(dest, '.git')):
        output = sh(
            ['git', 'clone', '--no-checkout', '-b', branch, repo, dest])
        click.echo('Cloned ...{repo_name}'.format(**locals()))

    else:
        # if there is a repo, make sure it's the right one
        current_remote, current_branch = get_repo_at(dest)
        repo = repo.lower()
        if not repo.endswith('.git'):
            repo += '.git'
        if not current_remote.endswith('.git'):
            current_remote += '.git'
        parsed_remote = urlparse(current_remote)
        parsed_repo = urlparse(repo)

        if (    parsed_repo.netloc != parsed_remote.netloc
                or parsed_repo.path != parsed_remote.path):
            raise ValueError(
                'Requested repo `...{repo_name}` but destination already '
                'has a remote repo cloned: {current_remote}'.format(**locals()))

        # and check that the branches match as well
        if branch.lower() != current_branch:
            raise ValueError(
                'Requested branch `{branch}` but destination is '
                'already on branch `{current_branch}`'.format(**locals()))

        # and check that we aren't going to overwrite any changes!
        # modified_status: uncommited modifications
        # ahead_status: commited but not pushed
        modified_status = sh(shlex.split('git status -s'), cwd=dest)
        ahead_status = sh(shlex.split('git status -sb'), cwd=dest)[3:]
        if modified_status:
            raise ValueError(
                'There are uncommitted changes at {dest} that syncing '
                'would overwrite'.format(**locals()))
        if '[ahead ' in ahead_status:
            raise ValueError(
                'This branch is ahead of the requested repo and syncing would '
                'overwrite the changes: {ahead_status}'.format(**locals()))


def sync_repo(repo, dest, branch, rev):
    """
    Syncs `branch` of remote `repo` (at `rev`) to `dest`.
    Assumes `dest` has already been cloned.
    """
    # fetch branch
    output = sh(['git', 'fetch', 'origin', branch], cwd=dest)
    click.echo('Fetched {branch}: {output}'.format(**locals()))

    # reset working copy
    if not rev:
        output = sh(['git', 'reset', '--hard', 'origin/' + branch], cwd=dest)
    else:
        output = sh(['git', 'reset', '--hard', rev], cwd=dest)

    # clean untracked files
    sh(['git', 'clean', '-dfq'], cwd=dest)

    click.echo('Reset to {rev}: {output}'.format(**locals()))

    repo_name = urlparse(repo).path
    click.echo(
        'Finished syncing {repo_name}:{branch} at {t:%Y-%m-%d %H:%M:%S}'.format(
            **locals(), t=datetime.datetime.now()))

@click.command()
@click.option('--dest', '-d', envvar='GIT_SYNC_DEST', default=os.getcwd(), help='The destination path. Defaults to the current working directory; can also be set with envvar GIT_SYNC_DEST.')
@click.option('--repo', '-r', envvar='GIT_SYNC_REPO', default='', help='The url of the remote repo to sync. Defaults to inferring from `dest`; can also be set with envvar GIT_SYNC_REPO.')
@click.option('--branch', '-b', envvar='GIT_SYNC_BRANCH', default='', help='The branch to sync. Defaults to inferring from `repo` (if already cloned), otherwise defaults to master; can also be set with envvar GIT_SYNC_BRANCH.')
@click.option('--wait', '-w', envvar='GIT_SYNC_WAIT', default=60, help='The number of seconds to pause after each sync. Defaults to 60; can also be set with envvar GIT_SYNC_WAIT.')
@click.option('--run-once', '-1', envvar='GIT_SYNC_RUN_ONCE', is_flag=True, help="Run only once (don't loop). Defaults to off; can also be set with envvar GIT_SYNC_RUN_ONCE=true.")
@click.option('--rev', envvar='GIT_SYNC_REV', default=None, help='The revision to sync. Defaults to HEAD; can also be set with envvar GIT_SYNC_REV.')
@click.option('--debug', envvar='GIT_SYNC_DEBUG', is_flag=True, help='Print tracebacks on error.')
def git_sync(repo, dest, branch, rev, wait, run_once, debug):
    """
    Periodically syncs a remote git repository to a local directory. The sync
    is one-way; any local changes will be lost.
    """

    if not debug:
        sys.excepthook = (
            lambda etype, e, tb : print("{}: {}".format(etype.__name__, e)))

    # infer repo/branch
    if not repo and not branch:
        repo, branch = get_repo_at(dest)
    elif not repo:
        repo, _ = get_repo_at(dest)
    elif not branch:
        branch = 'master'

    setup_repo(repo, dest, branch)
    while True:
        sync_repo(repo, dest, branch, rev)
        if run_once:
            break
        click.echo('Waiting {wait} seconds...'.format(**locals()))
        time.sleep(wait)

if __name__ == '__main__':
    git_sync()

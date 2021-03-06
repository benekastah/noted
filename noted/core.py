import configparser
import contextlib
import datetime
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile

import pytz
import tzlocal

from noted.sql import Model


MAX_TITLE_LEN = 80


class Journal(Model):
    name = 'journal'
    fields = {
        'created_at': datetime.datetime,
        'finished_at': datetime.datetime,
        'happened_at': datetime.datetime,
        'author': str,
        'title': str,
        'body': str
    }

    @property
    def created_at_local(self):
        if self.created_at is None:
            return
        return self.created_at.astimezone(tzlocal.get_localzone())

    @property
    def finished_at_local(self):
        if self.finished_at is None:
            return
        return self.finished_at.astimezone(tzlocal.get_localzone())

    @property
    def happened_at_local(self):
        if self.happened_at is None:
            return
        return self.happened_at.astimezone(tzlocal.get_localzone())


def get_config():
    home_dir = os.path.expanduser('~')
    if not get_config._conf:
        conf = configparser.ConfigParser()
        file_path = os.path.join(home_dir, '.config', 'noted', 'noted.cfg')
        if os.path.exists(file_path):
            conf.read(file_path)
        get_config._conf = conf
    return get_config._conf

get_config._conf = None


def shlex_split(cmd):
    # Good idea from https://github.com/MinchinWeb/jrnl/commit/281b74da743111e211a92131cfdee33a8700a180
    tuple(shlex.split(cmd, posix="win" not in sys.platform))


def get_editor():
    editor = get_config().get('editor')
    if editor:
        return shlex_split(editor)

    # Check a number of common environment variables for the user's preferred editor
    editor = os.environ.get('FCEDIT', os.environ.get('VISUAL', os.environ.get('EDITOR')))
    if editor:
        return shlex_split(editor)

    # Some editors listed in loose order from least likely to be installed
    # to most likely to be installed
    default_editors = (
        ('emacs',),
        ('atom', '--wait',),
        ('subl', '--wait',),
        ('nano',),
        ('pico',),
        ('vim',),
        ('vi',),
    )

    # If one of the above editors is found, use it
    for editor in default_editors:
        if shutil.which(editor[0]):
            return editor

    # Opens with the default editor in OSX
    if shutil.which('open'):
        return ('open', '-W', '-t',)
    # Opens with the default editor in Windows
    elif shutil.which('start'):
        return ('start', '/W', '""',)

    # Just return something, though it may not work
    return ('vi',)


def get_default_author():
    result = subprocess.run(('whoami',), stdout=subprocess.PIPE)
    return result.stdout.strip().decode() or None


@contextlib.contextmanager
def edit(content=None):
    with tempfile.NamedTemporaryFile('w+') as f:
        if content:
            f.write(content)
            f.flush()
            f.seek(0)
        editor = get_editor()
        subprocess.run(editor + (f.name,))
        yield f


def split_entry(entry):
    entry = entry.lstrip()
    title = []
    for ch in entry:
        if ch in ('\n', '\r'):
            break
        title.append(ch)
        if len(title) >= MAX_TITLE_LEN:
            title.append('\u2026')  # Add ellipsis to the end
            break
    title = ''.join(title)
    body = entry[len(title):].strip()
    return title, body


def add_journal_entry(author, happened_at=None):
    created_at = datetime.datetime.now(pytz.utc)

    with edit() as f:
        finished_at = datetime.datetime.now(pytz.utc)
        entry = f.read()
        title, body = split_entry(entry)
        entry = Journal(
            created_at=created_at,
            finished_at=finished_at,
            happened_at=happened_at,
            author=author,
            title=title,
            body=body)
        entry.save()

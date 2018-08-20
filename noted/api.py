from noted import core

import tzlocal


class API:
    def add(self, author=None):
        if author is None:
            author = core.get_default_author()
        core.add_journal_entry(author)

    def show(self):
        tz = tzlocal.get_localzone()
        for entry in core.Journal.query():
            print(entry.created_at.astimezone(tz).strftime('%Y-%m-%d %H:%M'), entry.title)

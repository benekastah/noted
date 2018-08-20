import fire

from noted import core
from noted import api

core.Journal.create_table()

fire.Fire(api.API)

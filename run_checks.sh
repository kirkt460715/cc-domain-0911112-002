#!/bin/sh
set -eu
python3 -m compileall -q hangar_ingest
python3 -m unittest discover -s tests -v
python3 -m hangar_ingest --db :memory: demo --fixture fixtures/demo.json
python3 -m synthetic_exchange.smoke

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from hangar_ingest.types import PipelineError, canonical, fail
from hangar_ingest.api import Application
from hangar_ingest.runs.database import Database
from hangar_ingest.runs import batches, inspection, processor
from hangar_ingest.exports import releases, tabular


def make_parser():
    parser = argparse.ArgumentParser(prog='hangar-ingest')
    parser.add_argument('--db', default='hangar.sqlite3')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('init')
    sub.add_parser('commands')
    sub.add_parser('check')
    call = sub.add_parser('call')
    call.add_argument('operation')
    call.add_argument('--json', default='{}')
    call.add_argument('--input')
    ingest = sub.add_parser('ingest')
    ingest.add_argument('path')
    ingest.add_argument('--batch', required=True)
    ingest.add_argument('--source', required=True)
    ingest.add_argument('--format', choices=['csv', 'ndjson'], default='ndjson')
    ingest.add_argument('--chunk', type=int, default=100)
    ingest.add_argument('--stage-only', action='store_true')
    batch = sub.add_parser('batch')
    batch.add_argument('path')
    export = sub.add_parser('export')
    export.add_argument('release_id')
    export.add_argument('destination')
    export.add_argument('--format', choices=['directory', 'csv', 'ndjson'], default='directory')
    demo = sub.add_parser('demo')
    demo.add_argument('--fixture', required=True)
    return parser


def read(path, limit=64 * 1024 * 1024):
    file = Path(path)
    if not file.is_file() or file.is_symlink() or file.stat().st_size > limit:
        fail('input_file', 'Input must be a bounded regular file')
    data = file.read_bytes()
    if len(data) > limit:
        fail('input_file', 'Input expanded beyond size limit')
    return data


def json_input(path):
    try:
        return json.loads(read(path))
    except (ValueError, UnicodeError) as exc:
        raise PipelineError('input_json', 'Input is not valid UTF-8 JSON') from exc


def write_new(path, content):
    file = Path(path)
    if file.exists() or file.is_symlink():
        fail('output_exists', 'Output already exists')
    import os
    with file.open('xb') as output:
        output.write(content)
        output.flush()
        os.fsync(output.fileno())
    return {'path': str(file), 'bytes': len(content)}


def demo(app, fixture):
    from hangar_ingest.types import object_fields
    object_fields(fixture, {'setup', 'batch', 'expected', 'release'})
    app.batch(fixture['setup'])
    batch = fixture['batch']
    staged = app.call('batch.stage', batch)
    processed = app.call('batch.drain', {'batch_id': staged['batch_id'], 'chunk': 2})
    app.call('partition.rebuild')
    app.call('watermark.advance', {'source': batch['source'], 'watermark': fixture['release']['watermark']})
    app.call('pipeline.publish', {'release_id': fixture['release']['release_id'], 'partition_keys': fixture['release']['partition_keys']})
    metrics = inspection.overview(app.db)
    for key, expected in fixture['expected'].items():
        if metrics.get(key) != expected:
            fail('demo_result', 'Demo observable result differs', field=key, expected=expected, actual=metrics.get(key))
    checked = inspection.check(app.db)
    if not checked['ok']:
        fail('demo_integrity', 'Demo integrity checks failed', errors=checked['errors'])
    package = releases.payloads(app.db, fixture['release']['release_id'])
    verified = releases.verify_package(package)
    return {'ok': True, 'metrics': metrics, 'processed': processed, 'verification': verified, 'integrity': checked}


def dispatch(app, args):
    if args.command == 'init':
        return {'initialized': True, 'integrity': app.db.integrity()}
    if args.command == 'commands':
        return app.commands()
    if args.command == 'check':
        return inspection.check(app.db)
    if args.command == 'call':
        try:
            parameters = json_input(args.input) if args.input else json.loads(args.json)
        except ValueError as exc:
            raise PipelineError('input_json', 'Parameters are invalid JSON') from exc
        return app.call(args.operation, parameters)
    if args.command == 'batch':
        return app.batch(json_input(args.path))
    if args.command == 'ingest':
        staged = batches.stage(app.db, args.batch, args.source, read(args.path), args.format)
        if args.stage_only:
            return staged
        return processor.drain(app.db, staged['batch_id'], chunk=args.chunk)
    if args.command == 'export':
        if args.format == 'directory':
            return releases.export_directory(app.db, args.release_id, args.destination)
        content = tabular.csv_bytes(app.db, args.release_id) if args.format == 'csv' else tabular.ndjson_bytes(app.db, args.release_id)
        return write_new(args.destination, content)
    if args.command == 'demo':
        return demo(app, json_input(args.fixture))
    fail('unknown_command', 'Unknown command')


def main(argv=None, stdout=None, stderr=None):
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    args = make_parser().parse_args(argv)
    try:
        with Database(args.db) as db:
            result = dispatch(Application(db), args)
        stdout.write(canonical({'ok': True, 'result': result}) + '\n')
        return 2 if isinstance(result, dict) and result.get('ok') is False else 0
    except PipelineError as exc:
        stderr.write(canonical({'ok': False, 'error': exc.as_dict()}) + '\n')
        return 2
    except (OSError, sqlite3.Error) as exc:
        stderr.write(canonical({'ok': False, 'error': {'code': 'storage_error', 'message': str(exc)}}) + '\n')
        return 3

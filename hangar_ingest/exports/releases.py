import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from hangar_ingest.types import canonical, digest, fail, identity
from hangar_ingest.exports import watermarks
from hangar_ingest.runs import journal, partitions


def prepare(db, release_id, partition_keys, *, require_watermark=True):
    identity(release_id, 'release_id')
    if not isinstance(partition_keys, list) or not partition_keys or len(partition_keys) > 1000:
        fail('release_partitions', 'Release requires a bounded nonempty partition list')
    if len(partition_keys) != len(set(partition_keys)):
        fail('release_partitions', 'Release contains duplicate partition keys')
    with db.transaction():
        existing = db.one('SELECT * FROM releases WHERE release_id=?', (release_id,))
        if existing:
            fail('release_exists', 'Release ID is already reserved')
        entries = []
        for partition_key in sorted(partition_keys):
            current = partitions.get(db, partition_key)
            if current['state'] != 'ready':
                fail('partition_dirty', 'Release requires materialized clean partitions', partition_key=partition_key)
            version = partitions.version(db, partition_key)
            require_current_calibration(db, version['payload'])
            readiness = watermarks.readiness(db, partition_key)
            if require_watermark and not readiness['ready']:
                fail('watermark_pending', 'Partition inputs are not sealed by watermarks', readiness=readiness)
            entries.append({
                'partition_key': partition_key,
                'revision': version['revision'],
                'digest': version['digest'],
                'count': len(version['payload']['observations']),
                'watermark': readiness['watermark'],
            })
        manifest = {'format': 'hangar-partitions', 'version': 1, 'release_id': release_id, 'sealed': require_watermark, 'partitions': entries}
        sha = digest(manifest)
        db.execute(
            "INSERT INTO releases(release_id,state,manifest,digest,created_at) VALUES(?,'prepared',?,?,?)",
            (release_id, canonical(manifest), sha, db.clock()),
        )
        journal.record(db, 'release.prepared', {'release_id': release_id, 'digest': sha, 'partitions': len(entries)})
        return get(db, release_id)


def get(db, release_id):
    row = db.one('SELECT * FROM releases WHERE release_id=?', (release_id,))
    if row is None:
        fail('unknown_release', 'Release does not exist')
    row['manifest'] = json.loads(row['manifest'])
    if digest(row['manifest']) != row['digest']:
        fail('release_corrupt', 'Release manifest hash mismatch')
    return row


def commit(db, release_id):
    with db.transaction():
        release = get(db, release_id)
        if release['state'] == 'committed':
            return release
        if release['state'] != 'prepared':
            fail('release_state', 'Release cannot be committed')
        for entry in release['manifest']['partitions']:
            current = partitions.get(db, entry['partition_key'])
            if current['state'] != 'ready' or current['revision'] != entry['revision']:
                fail('release_stale', 'Partition changed after release preparation', partition_key=entry['partition_key'])
            actual = partitions.version(db, entry['partition_key'], entry['revision'])
            require_current_calibration(db, actual['payload'])
            if actual['digest'] != entry['digest']:
                fail('release_corrupt', 'Partition checksum changed')
            if release['manifest']['sealed'] and not watermarks.readiness(db, entry['partition_key'])['ready']:
                fail('release_unsealed', 'New pending input invalidated release seal')
        db.execute("UPDATE releases SET state='committed',committed_at=? WHERE release_id=?", (db.clock(), release_id))
        journal.record(db, 'release.committed', {'release_id': release_id, 'digest': release['digest']})
        return get(db, release_id)


def abort(db, release_id, reason):
    from hangar_ingest.types import text
    reason = text(reason, 'reason')
    with db.transaction():
        release = get(db, release_id)
        if release['state'] != 'prepared':
            fail('release_state', 'Only prepared release can be aborted')
        db.execute("UPDATE releases SET state='aborted' WHERE release_id=?", (release_id,))
        journal.record(db, 'release.aborted', {'release_id': release_id, 'reason': reason})
        return get(db, release_id)


def payloads(db, release_id):
    release = get(db, release_id)
    if release['state'] != 'committed':
        fail('release_uncommitted', 'Only committed release can be exported')
    values = []
    for entry in release['manifest']['partitions']:
        version = partitions.version(db, entry['partition_key'], entry['revision'])
        values.append({'partition_key': entry['partition_key'], 'revision': entry['revision'], 'payload': version['payload'], 'digest': version['digest']})
    return {'manifest': release['manifest'], 'digest': release['digest'], 'partitions': values}


def verify_package(value):
    from hangar_ingest.types import object_fields
    object_fields(value, {'manifest', 'digest', 'partitions'})
    if digest(value['manifest']) != value['digest']:
        fail('package_digest', 'Package manifest digest mismatch')
    expected = {(entry['partition_key'], entry['revision']): entry for entry in value['manifest']['partitions']}
    if len(expected) != len(value['partitions']):
        fail('package_count', 'Package partition count mismatch')
    seen = set()
    for item in value['partitions']:
        key = (item['partition_key'], item['revision'])
        if key not in expected or key in seen:
            fail('package_key', 'Unexpected or duplicate partition')
        seen.add(key)
        if digest(item['payload']) != item['digest'] or item['digest'] != expected[key]['digest']:
            fail('package_partition', 'Partition checksum mismatch', partition_key=key[0])
        if len(item['payload']['observations']) != expected[key]['count']:
            fail('package_rows', 'Partition row count mismatch')
    return {'ok': True, 'partitions': len(seen), 'digest': value['digest']}


def export_directory(db, release_id, destination):
    value = payloads(db, release_id)
    verify_package(value)
    target = Path(destination)
    if target.exists() or target.is_symlink() or not target.parent.is_dir():
        fail('export_destination', 'Destination must be a new directory below an existing parent')
    temporary = Path(tempfile.mkdtemp(prefix='.' + target.name + '.', dir=target.parent))
    try:
        content = canonical(value).encode() + b'\n'
        file = temporary / 'release.json'
        with file.open('wb') as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        if target.exists():
            fail('export_destination', 'Destination appeared during export')
        os.rename(temporary, target)
        descriptor = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return {'path': str(target), 'bytes': len(content), 'sha256': hashlib.sha256(content).hexdigest()}
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def list_releases(db):
    return [get(db, row['release_id']) for row in db.all('SELECT release_id FROM releases ORDER BY created_at,release_id')]


def require_current_calibration(db, payload):
    for observation in payload['observations']:
        calibration_id = observation['calibration_id']
        if calibration_id is None:
            continue
        current = db.one('SELECT revision FROM calibrations WHERE calibration_id=?', (calibration_id,))
        if current is None or current['revision'] != observation['calibration_revision']:
            fail('release_calibration_stale', 'Release references an outdated calibration', observation_id=observation['observation_id'])

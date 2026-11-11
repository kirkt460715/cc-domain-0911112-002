from hangar_ingest.types import fail, identity, instant, integer
from hangar_ingest.runs import journal
from hangar_ingest.runs.partitions import day_bounds, split


def advance(db, source, watermark, revision=0):
    identity(source, 'source')
    watermark = instant(watermark)
    integer(revision, 'revision')
    with db.transaction():
        previous = db.one('SELECT * FROM source_watermarks WHERE source=?', (source,))
        expected = previous['revision'] if previous else 0
        if revision != expected:
            fail('stale_revision', 'Source watermark revision changed')
        if previous and watermark < previous['watermark']:
            fail('watermark_backwards', 'Source watermark cannot move backwards')
        if previous and watermark == previous['watermark']:
            return previous
        db.execute(
            'INSERT INTO source_watermarks(source,watermark,revision) VALUES(?,?,?) '
            'ON CONFLICT(source) DO UPDATE SET watermark=excluded.watermark,revision=excluded.revision',
            (source, watermark, revision + 1),
        )
        journal.record(db, 'watermark.advanced', {'source': source, 'watermark': watermark, 'revision': revision + 1})
        return db.one('SELECT * FROM source_watermarks WHERE source=?', (source,))


def partition_sources(db, partition_key):
    aircraft, day = split(partition_key)
    start, end = day_bounds(day)
    rows = db.all(
        'SELECT DISTINCT a.source FROM aliases a JOIN devices d ON a.device_id=d.device_id '
        'WHERE d.aircraft=? AND a.valid_from<? AND a.valid_until>? ORDER BY a.source',
        (aircraft, end, start),
    )
    return [row['source'] for row in rows]


def readiness(db, partition_key):
    aircraft, day = split(partition_key)
    _, end = day_bounds(day)
    sources = partition_sources(db, partition_key)
    missing = []
    values = []
    for source in sources:
        row = db.one('SELECT * FROM source_watermarks WHERE source=?', (source,))
        if row is None or row['watermark'] < end:
            missing.append({'source': source, 'watermark': row['watermark'] if row else None, 'required': end})
        if row:
            values.append(row['watermark'])
    pending = db.all(
        "SELECT batch_id,source,state FROM batches WHERE state IN ('staged','processing','paused') ORDER BY batch_id",
    )
    pending = [row for row in pending if row['source'] in sources]
    return {
        'partition_key': partition_key,
        'ready': bool(sources) and not missing and not pending,
        'sources': sources,
        'missing_watermarks': missing,
        'pending_batches': pending,
        'watermark': min(values) if values and len(values) == len(sources) else None,
    }


def list_watermarks(db):
    return db.all('SELECT * FROM source_watermarks ORDER BY source')

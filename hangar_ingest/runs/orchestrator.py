from hangar_ingest.types import fail, identity, integer
from hangar_ingest.runs import batches, claims, journal, partitions, processor
from hangar_ingest.exports import releases, watermarks


class Pipeline:
    def __init__(self, db):
        self.db = db

    def ingest(self, batch_id, source, content, *, format='ndjson', chunk=100):
        staged = batches.stage(self.db, batch_id, source, content, format)
        actual_id = staged['batch_id']
        result = processor.drain(self.db, actual_id, chunk=chunk)
        return {'staged': staged, 'result': result}

    def run_claimed(self, batch_id, owner, *, limit=100, ttl=30):
        token = claims.acquire(self.db, batch_id, owner, ttl)
        try:
            result = processor.process(self.db, batch_id, limit=limit, claim=token)
        except BaseException:
            claims.release(self.db, batch_id, token)
            raise
        claims.release(self.db, batch_id, token)
        return result

    def recover(self, *, owner='recovery', chunk=100, limit=100):
        integer(limit, 'limit', 1, 10000)
        selected = [row for row in batches.listing(self.db) if row['state'] in {'staged', 'processing'}][:limit]
        result = []
        for row in selected:
            outcome = self.run_claimed(row['batch_id'], owner, limit=chunk)
            result.append({'batch_id': row['batch_id'], **outcome})
        return result

    def materialize(self, *, limit=100):
        return partitions.rebuild_dirty(self.db, limit)

    def publish(self, release_id, partition_keys, *, require_watermark=True):
        with self.db.transaction():
            prepared = releases.prepare(self.db, release_id, partition_keys, require_watermark=require_watermark)
            committed = releases.commit(self.db, release_id)
            return {'prepared_digest': prepared['digest'], 'release': committed}

    def finish_source_day(self, source, day_end, watermark_revision, release_id, partition_keys):
        with self.db.transaction():
            pending = [row for row in batches.listing(self.db) if row['source'] == source and row['state'] in {'staged', 'processing', 'paused'}]
            if pending:
                fail('source_pending', 'Cannot seal source while batches remain pending', batches=[row['batch_id'] for row in pending])
            watermarks.advance(self.db, source, day_end, watermark_revision)
            versions = []
            for partition_key in partition_keys:
                versions.append(partitions.materialize(self.db, partition_key))
            result = self.publish(release_id, partition_keys)
            return {'versions': versions, **result}

    def status(self):
        from hangar_ingest.runs.inspection import overview
        return overview(self.db)

-- This second external script follows the inline legacy version 2 repair.
-- Filenames do not change the recorded database schema version.
ALTER TABLE archives ADD COLUMN min_id BIGINT;
ALTER TABLE archives ADD COLUMN max_id BIGINT;
ALTER TABLE archives ADD COLUMN file_bytes UBIGINT;
ALTER TABLE archives ADD COLUMN schema_version INTEGER DEFAULT 1;

INSERT INTO schema_version VALUES (3);

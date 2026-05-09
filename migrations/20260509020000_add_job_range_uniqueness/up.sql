CREATE UNIQUE INDEX idx_jobs_unique_source_type_range
    ON jobs(source_id, job_type, from_block, to_block)
    WHERE source_id IS NOT NULL
      AND from_block IS NOT NULL
      AND to_block IS NOT NULL;

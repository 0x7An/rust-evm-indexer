use chrono::{Duration, Utc};
use diesel::{
    PgConnection, QueryableByName,
    prelude::*,
    sql_types::{BigInt, Nullable, Text, Uuid as SqlUuid},
};
use uuid::Uuid;

use crate::domain::job::{JobStatus, JobType};

use super::{
    connection::PgPool,
    models::{JobAttemptRow, JobRow, NewJobAttemptRow, NewJobRow},
    schema::{job_attempts, jobs},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewJob {
    pub id: Uuid,
    pub job_type: JobType,
    pub source_id: Option<Uuid>,
    pub chain_id: i64,
    pub from_block: Option<i64>,
    pub to_block: Option<i64>,
    pub idempotency_key: String,
    pub max_attempts: i32,
}

impl NewJob {
    pub fn new(job_type: JobType, chain_id: i64, idempotency_key: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            job_type,
            source_id: None,
            chain_id,
            from_block: None,
            to_block: None,
            idempotency_key: idempotency_key.into(),
            max_attempts: 5,
        }
    }

    pub fn with_range(mut self, from_block: i64, to_block: i64) -> Self {
        self.from_block = Some(from_block);
        self.to_block = Some(to_block);
        self
    }

    pub fn with_source(mut self, source_id: Uuid) -> Self {
        self.source_id = Some(source_id);
        self
    }

    pub fn with_max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts;
        self
    }
}

impl From<NewJob> for NewJobRow {
    fn from(job: NewJob) -> Self {
        Self {
            id: job.id,
            job_type: job.job_type.to_string(),
            status: JobStatus::Queued.to_string(),
            source_id: job.source_id,
            chain_id: job.chain_id,
            from_block: job.from_block,
            to_block: job.to_block,
            idempotency_key: job.idempotency_key,
            max_attempts: job.max_attempts,
        }
    }
}

#[derive(Debug, Clone)]
pub enum EnqueueResult {
    Inserted(JobRow),
    Existing(JobRow),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobStatusCount {
    pub status: String,
    pub count: i64,
}

#[derive(Debug, QueryableByName)]
struct JobStatusCountRow {
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(Clone)]
pub struct JobRepository {
    pool: PgPool,
}

impl JobRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn enqueue(&self, job: NewJob) -> QueryResult<EnqueueResult> {
        let mut conn = self.connection()?;
        let new_row = NewJobRow::from(job);

        let inserted = diesel::insert_into(jobs::table)
            .values(&new_row)
            .on_conflict_do_nothing()
            .get_result::<JobRow>(&mut conn)
            .optional()?;

        match inserted {
            Some(row) => Ok(EnqueueResult::Inserted(row)),
            None => self
                .existing_enqueue_conflict(&mut conn, &new_row)
                .map(EnqueueResult::Existing),
        }
    }

    fn existing_enqueue_conflict(
        &self,
        conn: &mut PgConnection,
        new_row: &NewJobRow,
    ) -> QueryResult<JobRow> {
        let by_idempotency_key = jobs::table
            .filter(jobs::idempotency_key.eq(&new_row.idempotency_key))
            .first::<JobRow>(conn)
            .optional()?;
        if let Some(row) = by_idempotency_key {
            return Ok(row);
        }

        if let (Some(source_id), Some(from_block), Some(to_block)) =
            (new_row.source_id, new_row.from_block, new_row.to_block)
        {
            return jobs::table
                .filter(jobs::source_id.eq(Some(source_id)))
                .filter(jobs::job_type.eq(&new_row.job_type))
                .filter(jobs::from_block.eq(Some(from_block)))
                .filter(jobs::to_block.eq(Some(to_block)))
                .first::<JobRow>(conn);
        }

        Err(diesel::result::Error::NotFound)
    }

    pub fn lease_next(&self, worker_id: &str, lease_for: Duration) -> QueryResult<Option<JobRow>> {
        self.lease_next_candidate(worker_id, lease_for, None, None)
    }

    pub fn lease_next_for_type(
        &self,
        worker_id: &str,
        lease_for: Duration,
        job_type: JobType,
    ) -> QueryResult<Option<JobRow>> {
        self.lease_next_candidate(worker_id, lease_for, Some(job_type), None)
    }

    pub fn lease_next_for_type_and_chain(
        &self,
        worker_id: &str,
        lease_for: Duration,
        job_type: JobType,
        chain_id: i64,
    ) -> QueryResult<Option<JobRow>> {
        self.lease_next_candidate(worker_id, lease_for, Some(job_type), Some(chain_id))
    }

    fn lease_next_candidate(
        &self,
        worker_id: &str,
        lease_for: Duration,
        job_type: Option<JobType>,
        chain_id: Option<i64>,
    ) -> QueryResult<Option<JobRow>> {
        let mut conn = self.connection()?;
        conn.transaction(|conn| {
            let candidate = self.lock_next_candidate(conn, job_type, chain_id)?;

            let Some(candidate) = candidate else {
                return Ok(None);
            };

            let now = Utc::now();
            if matches!(candidate.status.as_str(), "leased" | "running")
                && candidate
                    .lease_expires_at
                    .as_ref()
                    .is_some_and(|expires_at| *expires_at < now)
            {
                self.update_attempt_status(
                    conn,
                    candidate.id,
                    candidate.attempts,
                    JobStatus::Cancelled,
                    Some("LeaseExpired".to_string()),
                    Some("job lease expired before worker completed".to_string()),
                )?;
            }

            let leased = diesel::update(jobs::table.filter(jobs::id.eq(candidate.id)))
                .set((
                    jobs::status.eq(JobStatus::Leased.to_string()),
                    jobs::leased_by.eq(Some(worker_id.to_string())),
                    jobs::lease_expires_at.eq(Some(now + lease_for)),
                    jobs::attempts.eq(candidate.attempts + 1),
                    jobs::error_class.eq::<Option<String>>(None),
                    jobs::error_message.eq::<Option<String>>(None),
                    jobs::updated_at.eq(now),
                ))
                .get_result::<JobRow>(conn)?;

            diesel::insert_into(job_attempts::table)
                .values(NewJobAttemptRow {
                    id: Uuid::new_v4(),
                    job_id: leased.id,
                    attempt_number: leased.attempts,
                    worker_id: worker_id.to_string(),
                    status: JobStatus::Leased.to_string(),
                })
                .execute(conn)?;

            Ok(Some(leased))
        })
    }

    pub fn mark_interrupted_for_retry(
        &self,
        job_id: Uuid,
        error_message: &str,
    ) -> QueryResult<JobRow> {
        let mut conn = self.connection()?;
        conn.transaction(|conn| {
            let row = jobs::table
                .filter(jobs::id.eq(job_id))
                .for_update()
                .first::<JobRow>(conn)?;

            if !matches!(row.status.as_str(), "leased" | "running") {
                return Ok(row);
            }

            let now = Utc::now();
            let row = diesel::update(jobs::table.filter(jobs::id.eq(row.id)))
                .set((
                    jobs::status.eq(JobStatus::Queued.to_string()),
                    jobs::leased_by.eq::<Option<String>>(None),
                    jobs::lease_expires_at.eq::<Option<chrono::DateTime<Utc>>>(None),
                    jobs::error_class.eq(Some("WorkerInterrupted".to_string())),
                    jobs::error_message.eq(Some(error_message.to_string())),
                    jobs::updated_at.eq(now),
                ))
                .get_result::<JobRow>(conn)?;

            self.update_attempt_status(
                conn,
                row.id,
                row.attempts,
                JobStatus::Cancelled,
                Some("WorkerInterrupted".to_string()),
                Some(error_message.to_string()),
            )?;

            Ok(row)
        })
    }

    pub fn mark_running(&self, job_id: Uuid) -> QueryResult<JobRow> {
        let mut conn = self.connection()?;
        conn.transaction(|conn| {
            let row = diesel::update(jobs::table.filter(jobs::id.eq(job_id)))
                .set((
                    jobs::status.eq(JobStatus::Running.to_string()),
                    jobs::updated_at.eq(Utc::now()),
                ))
                .get_result::<JobRow>(conn)?;

            self.update_attempt_status(conn, row.id, row.attempts, JobStatus::Running, None, None)?;

            Ok(row)
        })
    }

    pub fn mark_succeeded(&self, job_id: Uuid) -> QueryResult<JobRow> {
        let mut conn = self.connection()?;
        conn.transaction(|conn| {
            let now = Utc::now();
            let row = diesel::update(jobs::table.filter(jobs::id.eq(job_id)))
                .set((
                    jobs::status.eq(JobStatus::Succeeded.to_string()),
                    jobs::leased_by.eq::<Option<String>>(None),
                    jobs::lease_expires_at.eq::<Option<chrono::DateTime<Utc>>>(None),
                    jobs::updated_at.eq(now),
                ))
                .get_result::<JobRow>(conn)?;

            self.update_attempt_status(
                conn,
                row.id,
                row.attempts,
                JobStatus::Succeeded,
                None,
                None,
            )?;

            Ok(row)
        })
    }

    pub fn mark_failed(
        &self,
        job_id: Uuid,
        error_class: &str,
        error_message: &str,
    ) -> QueryResult<JobRow> {
        let mut conn = self.connection()?;
        conn.transaction(|conn| {
            let row = jobs::table
                .filter(jobs::id.eq(job_id))
                .for_update()
                .first::<JobRow>(conn)?;

            let next_status = if row.attempts >= row.max_attempts {
                JobStatus::DeadLettered
            } else {
                JobStatus::Queued
            };
            let retry = next_status != JobStatus::DeadLettered;
            tracing::warn!(
                job_id = %row.id,
                job_type = %row.job_type,
                error_class,
                attempts = row.attempts,
                max_attempts = row.max_attempts,
                retry,
                "job attempt failed"
            );

            let now = Utc::now();
            let row = diesel::update(jobs::table.filter(jobs::id.eq(row.id)))
                .set((
                    jobs::status.eq(next_status.to_string()),
                    jobs::leased_by.eq::<Option<String>>(None),
                    jobs::lease_expires_at.eq::<Option<chrono::DateTime<Utc>>>(None),
                    jobs::error_class.eq(Some(error_class.to_string())),
                    jobs::error_message.eq(Some(error_message.to_string())),
                    jobs::updated_at.eq(now),
                ))
                .get_result::<JobRow>(conn)?;

            let attempt_status = if next_status == JobStatus::DeadLettered {
                JobStatus::DeadLettered
            } else {
                JobStatus::Failed
            };

            self.update_attempt_status(
                conn,
                row.id,
                row.attempts,
                attempt_status,
                Some(error_class.to_string()),
                Some(error_message.to_string()),
            )?;

            Ok(row)
        })
    }

    pub fn get(&self, job_id: Uuid) -> QueryResult<JobRow> {
        let mut conn = self.connection()?;
        jobs::table.filter(jobs::id.eq(job_id)).first(&mut conn)
    }

    pub fn jobs_for_source(&self, source_id: Uuid) -> QueryResult<Vec<JobRow>> {
        let mut conn = self.connection()?;
        jobs::table
            .filter(jobs::source_id.eq(Some(source_id)))
            .order((
                jobs::from_block.asc(),
                jobs::to_block.asc(),
                jobs::created_at.asc(),
            ))
            .load(&mut conn)
    }

    pub fn status_counts(
        &self,
        chain_id: Option<i64>,
        source_id: Option<Uuid>,
        job_type: Option<JobType>,
    ) -> QueryResult<Vec<JobStatusCount>> {
        let mut conn = self.connection()?;
        let job_type = job_type.map(|value| value.to_string());

        diesel::sql_query(
            "SELECT status, COUNT(*)::bigint AS count
             FROM jobs
             WHERE ($1::bigint IS NULL OR chain_id = $1)
               AND ($2::uuid IS NULL OR source_id = $2)
               AND ($3::text IS NULL OR job_type = $3)
             GROUP BY status
             ORDER BY status",
        )
        .bind::<Nullable<BigInt>, _>(chain_id)
        .bind::<Nullable<SqlUuid>, _>(source_id)
        .bind::<Nullable<Text>, _>(job_type)
        .load::<JobStatusCountRow>(&mut conn)
        .map(|rows| {
            rows.into_iter()
                .map(|row| JobStatusCount {
                    status: row.status,
                    count: row.count,
                })
                .collect()
        })
    }

    pub fn attempts_for_job(&self, job_id: Uuid) -> QueryResult<Vec<JobAttemptRow>> {
        let mut conn = self.connection()?;
        job_attempts::table
            .filter(job_attempts::job_id.eq(job_id))
            .order(job_attempts::attempt_number.asc())
            .load(&mut conn)
    }

    fn connection(
        &self,
    ) -> Result<
        diesel::r2d2::PooledConnection<diesel::r2d2::ConnectionManager<PgConnection>>,
        diesel::result::Error,
    > {
        self.pool.get().map_err(|error| {
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::UnableToSendCommand,
                Box::new(error.to_string()),
            )
        })
    }

    #[tracing::instrument(
        name = "job_lease",
        skip_all,
        fields(
            job_type = tracing::field::Empty,
            chain_id = tracing::field::Empty,
        )
    )]
    fn lock_next_candidate(
        &self,
        conn: &mut PgConnection,
        job_type: Option<JobType>,
        chain_id: Option<i64>,
    ) -> QueryResult<Option<JobRow>> {
        let span = tracing::Span::current();
        span.record(
            "job_type",
            job_type.map(|value| value.as_str()).unwrap_or("any"),
        );
        if let Some(chain_id) = chain_id {
            span.record("chain_id", chain_id);
        }
        let now = Utc::now();

        match (job_type, chain_id) {
            (Some(job_type), Some(chain_id)) => jobs::table
                .filter(jobs::job_type.eq(job_type.to_string()))
                .filter(jobs::chain_id.eq(chain_id))
                .filter(
                    jobs::status
                        .eq(JobStatus::Queued.to_string())
                        .or(jobs::status
                            .eq_any([
                                JobStatus::Leased.to_string(),
                                JobStatus::Running.to_string(),
                            ])
                            .and(jobs::lease_expires_at.lt(now))),
                )
                .order(jobs::created_at.asc())
                .for_update()
                .skip_locked()
                .first::<JobRow>(conn)
                .optional(),
            (Some(job_type), None) => jobs::table
                .filter(jobs::job_type.eq(job_type.to_string()))
                .filter(
                    jobs::status
                        .eq(JobStatus::Queued.to_string())
                        .or(jobs::status
                            .eq_any([
                                JobStatus::Leased.to_string(),
                                JobStatus::Running.to_string(),
                            ])
                            .and(jobs::lease_expires_at.lt(now))),
                )
                .order(jobs::created_at.asc())
                .for_update()
                .skip_locked()
                .first::<JobRow>(conn)
                .optional(),
            (None, Some(chain_id)) => jobs::table
                .filter(jobs::chain_id.eq(chain_id))
                .filter(
                    jobs::status
                        .eq(JobStatus::Queued.to_string())
                        .or(jobs::status
                            .eq_any([
                                JobStatus::Leased.to_string(),
                                JobStatus::Running.to_string(),
                            ])
                            .and(jobs::lease_expires_at.lt(now))),
                )
                .order(jobs::created_at.asc())
                .for_update()
                .skip_locked()
                .first::<JobRow>(conn)
                .optional(),
            (None, None) => jobs::table
                .filter(
                    jobs::status
                        .eq(JobStatus::Queued.to_string())
                        .or(jobs::status
                            .eq_any([
                                JobStatus::Leased.to_string(),
                                JobStatus::Running.to_string(),
                            ])
                            .and(jobs::lease_expires_at.lt(now))),
                )
                .order(jobs::created_at.asc())
                .for_update()
                .skip_locked()
                .first::<JobRow>(conn)
                .optional(),
        }
    }

    fn update_attempt_status(
        &self,
        conn: &mut PgConnection,
        job_id: Uuid,
        attempt_number: i32,
        status: JobStatus,
        error_class: Option<String>,
        error_message: Option<String>,
    ) -> QueryResult<usize> {
        diesel::update(
            job_attempts::table
                .filter(job_attempts::job_id.eq(job_id))
                .filter(job_attempts::attempt_number.eq(attempt_number)),
        )
        .set((
            job_attempts::status.eq(status.to_string()),
            job_attempts::finished_at.eq(status.is_terminal().then(Utc::now)),
            job_attempts::error_class.eq(error_class),
            job_attempts::error_message.eq(error_message),
        ))
        .execute(conn)
    }
}

trait JobStatusExt {
    fn is_terminal(&self) -> bool;
}

impl JobStatusExt for JobStatus {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobStatus::Succeeded
                | JobStatus::Failed
                | JobStatus::DeadLettered
                | JobStatus::Cancelled
        )
    }
}

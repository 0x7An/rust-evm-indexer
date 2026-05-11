use indexer_rs::{
    application::{
        backfill::{BackfillChunk, plan_backfill_jobs},
        ports::SourceCheckpoint,
    },
    domain::job::JobType,
};

use crate::support::{FakeBackfillRepository, source};

#[test]
fn plans_deterministic_backfill_jobs_without_postgres() {
    let source = source(indexer_rs::application::evm::TokenStandard::Erc721);
    let repo = FakeBackfillRepository::default();

    let plan = plan_backfill_jobs(&repo, &source, 100, 109, 4, 3).expect("plan backfill");

    assert_eq!(plan.requested_from, 100);
    assert_eq!(plan.requested_to, 109);
    assert_eq!(plan.ranges.len(), 3);
    assert_eq!(plan.ranges[0], BackfillChunk { from: 100, to: 103 });
    assert_eq!(plan.ranges[2], BackfillChunk { from: 108, to: 109 });
    assert_eq!(plan.inserted_jobs, 3);
    assert_eq!(plan.existing_jobs, 0);

    let jobs = repo.jobs();
    assert_eq!(jobs.len(), 3);
    assert!(jobs.iter().all(|job| job.job_type == JobType::IngestRange));
    assert_eq!(
        jobs[0].idempotency_key,
        format!("backfill:{}:100:103", source.id)
    );

    let repeated = plan_backfill_jobs(&repo, &source, 100, 109, 4, 3).expect("repeat backfill");
    assert_eq!(repeated.inserted_jobs, 0);
    assert_eq!(repeated.existing_jobs, 3);
    assert_eq!(repo.jobs().len(), 3);
}

#[test]
fn resumes_backfill_after_checkpoint_without_postgres() {
    let source = source(indexer_rs::application::evm::TokenStandard::Erc721);
    let repo = FakeBackfillRepository::with_checkpoint(SourceCheckpoint {
        processed_block: 104,
        processed_block_hash: crate::support::block_hash("11"),
        finalized_block: 120,
    });

    let plan = plan_backfill_jobs(&repo, &source, 100, 109, 3, 3).expect("plan backfill");

    assert_eq!(plan.planned_from, Some(105));
    assert_eq!(plan.planned_to, Some(109));
    assert_eq!(
        plan.ranges,
        vec![
            BackfillChunk { from: 105, to: 107 },
            BackfillChunk { from: 108, to: 109 }
        ]
    );
}

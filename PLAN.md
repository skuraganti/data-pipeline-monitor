# Relay development and publication plan

Actual development/publication preparation2026-10-16. The full application is independent, useful and locally runnable. Scheduled NYC TLC ingestion, schema/domain checks, quarantine, lineage, bounded retries, checkpoints and recoverable publication.

Completed original milestones: define user/workflow/non-goals; compare real-data candidates; pin sources/licenses; scoped Claude implementation; lead integration and corrections; native/Docker/browser/persistence checks; record actual results and limitations. Public release adds a separate static browser demo, reviewed examples, reproducible bootstrap, portable configuration and publication checks before an initial commit.

Publication gates: clean-source/native tests after bootstrap, Docker health and real-data smoke workflow, Pages relative-path functionality/mobile/accessibility, credential/privacy/license review, truthful metadata, one initial commit, then sequential push/Pages deployment and public URL verification. A failure stops that repository's publication until corrected. No fabricated history or model-evaluation outcome is allowed.

The Pages queue/retry animation is explicitly simulated over actual captured aggregates. The local worker processes the original 56,551-row TLC file. Two invalid rows and 6,640 warning rows are retained rather than concealed.

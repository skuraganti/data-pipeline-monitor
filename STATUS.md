# Release status

**Local release gates: verified on 2026-10-16.** Theme 2022 is a technology theme, not a completion date. Source, native tests, Docker workflow/persistence, Pages browser/mobile/error states and publication privacy review passed. Actual publication is recorded by GitHub commit/deployment metadata after this prepared initial commit. See [VERIFICATION.md](VERIFICATION.md) for exact checks and limits.

Local: `sh stack.sh up` → http://127.0.0.1:8282. [Pages demo](https://skuraganti.github.io/2022-data-pipeline-monitor/) · [Source](https://github.com/skuraganti/2022-data-pipeline-monitor). Pages uses static browser interactions; full Python services run locally. Runtime databases, caches, model-control files and credentials stay excluded. The Pages queue/retry animation is explicitly simulated over actual captured aggregates. The local worker processes the original 56,551-row TLC file. Two invalid rows and 6,640 warning rows are retained rather than concealed.

Next maintenance action: use the documented setup, verify original-source checksums before updating fixtures, rerun affected tests and retain failed/uncertain evaluation outcomes. No hosted inference or unattended service is supplied by Pages.

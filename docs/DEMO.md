# Relay browser demonstration

Serve `docs` as static files, or run the full Python/Docker application using the [source repository instructions](https://github.com/skuraganti/2022-data-pipeline-monitor).

The fixture contains actual NYC TLC daily aggregate data, source URL, accepted/rejected/warning counts and five declared stages. The animated run, selected fault and retries are simulated only in the browser and bounded to three attempts. They do not ingest data, start a scheduler or demonstrate server recovery. Failed replays leave the loaded aggregate untouched; reset clears only browser simulation.

Source: [NYC TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The fixture does not expose an input source SHA-256; the interface states that rather than inventing one.

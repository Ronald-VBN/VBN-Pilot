# Dagster

Dagster orchestration service aligned with the Dagster OSS Docker Compose
pattern (webserver + daemon), with pluggable storage bindings.

## Purpose

Provides a code server, webserver, and daemon for profiles that need
orchestration. The webserver and daemon share the user-code gRPC socket
and the configured storage bindings.

## Known limitations

- `runStorage`, `eventLogStorage`, and `scheduleStorage` are mandatory bindings in the config schema
- `definitionsFile.hostPath` is bind-mounted read-only into the user-code container, the host file must exist before `cds up`
- `sharedData.hostPath` is bind-mounted into the containers, the host directory must exist before `cds up`
- The task daemon runs only when `daemon.enabled` is true, schedules and sensors need it

## Production and learning variants

When `config.image.variant` is omitted, the planner selects `base` for
learning/development profiles and `hardened` for production profiles. An
explicit `config.image.variant` takes precedence over that environment
default, so either image can still be tested in either environment. The
`--hardened` CLI option remains an explicit override that forces the hardened
variant. For registry images, the resolved variant also prefixes the default
tag (for example, `hardened-1.13.26`).

In production profiles the webserver's host port publish is cleared
(`production: { ports: [] }` on the compose service): the TLS reverse proxy
is the sole host-published entry point and `CDS-SEC-074` does not fire
(issue #981). Learning/dev profiles keep the direct `webPort:3000` publish
for local access. The Kubernetes target always declares the container port
via `containerOverrides`, independent of the compose host publish.

## Upstream documentation

- [Dagster documentation](https://docs.dagster.io/)

## Configuration notes

- `storage.backend` selects the database driver for the local image build and must match the database behind the storage bindings
- The webserver and daemon wait for the user-code healthcheck over a Unix socket, start period is 60 seconds

## EmballageTool migration

The `load_emballagetool_stg_to_his` job migrates ADF pipeline
`02e_Load_STG_and_HIS_data_EmballageTool` to the Postgres analytics database
already configured for Dagster. It removes duplicates from
`stg.tbl_emballagetool_data` using the ADF partition key
(`verzender`, `datum`, `ontvanger`, `gebruiker`), upserts the rows into
`his.tbl_emballagetool_data`, and truncates staging only after the upsert
succeeds. The history table is created on first run; the staging table must
be populated by the upstream ingestion process.

The Postgres upsert replaces the SQL Server-generated merge procedures for
the EmballageTool `Data` table. Its composite key follows the key columns
declared by `02e_Get_Metadata_EmballageTool`; the field names/types follow
the staging table definition in `01e_ImportToStaging_EmballageTool`. The
pipeline uses the existing `CDS_ANALYTICS_DB_CONNECTION_URI` injected by the
Dagster/Postgres profile; no SQL Server driver or extra connection secret is
needed. The ADF trigger configuration was not part of the pipeline
definition, so no schedule is added.

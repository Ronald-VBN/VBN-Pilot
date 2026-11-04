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
`02e_Load_STG_and_HIS_data_EmballageTool`. It looks up the source tables in
`[generate].[SourceColumns]`, removes duplicates from
`[STG].[tbl_EmballageTool_Data]`, calls each matching
`[HIS].[sp_<SourceSchema>_<SourceTable>_Merge]` procedure sequentially with
`IsDeltaload = true`, then calls `[STG].[sp_Empty_STG_Tabels]`. The lookup and
deduplication run as independent steps; merging waits for both to succeed.
The source schema defaults to `EmballageTool` and can be overridden in the
`EMBALLAGETOOL_SOURCE_SCHEMA` environment variable.

Set `EMBALLAGETOOL_DWH_CONNECTION_URI` for both Dagster user-code and daemon
services. It must be a SQLAlchemy SQL Server URI, for example:

```text
mssql+pyodbc://<user>:<password>@<host>:1433/<database>?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes
```

Supply the URI through the deployment's secret mechanism, not in source
control. The corresponding SQLAlchemy DBAPI driver and any native driver
dependencies (for example, `pyodbc` and Microsoft ODBC Driver 18) must be
installed in the Dagster image. The migration does not add a schedule because
the ADF trigger configuration was not part of the pipeline definition.

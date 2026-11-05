# User Guide

## 1. Prerequisites
- Python 3.14 (3.10+ should work), Java JDK 21, PostgreSQL 18 with `psql`
- Windows only: Hadoop native binaries (see section 3)
- Internet access on the first run (Spark downloads the PostgreSQL JDBC driver)

## 2. Setup
```powershell
git clone <repo-url>
cd realtime-ecommerce-streaming
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```
If PowerShell blocks activation:
`Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`

## 3. Windows: Hadoop binaries
Spark streaming on Windows needs `hadoop.dll` and `winutils.exe`.
1. Place them in `C:\hadoop\bin` (use a build that matches Hadoop 3.x).
2. Set the user environment variable `HADOOP_HOME=C:\hadoop` and add `C:\hadoop\bin` to `Path`.
3. Restart VS Code and all terminals.
Without this, the stream fails with `UnsatisfiedLinkError ... NativeIO$Windows.access0`.

## 4. Create the database
```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -f postgres_setup.sql
```
This creates the `streaming_user` role, the `ecommerce_streaming` database and the `events` table.
Connection details are in `postgres_connection_details.txt`.
To use a different password, set `PG_USER` and `PG_PASSWORD` before running the stream.

## 5. Run the pipeline
Use two terminals, both with the venv activated.

Terminal 1 (start first and wait for it to start):
```powershell
python spark_streaming_to_postgres.py
```
Terminal 2:
```powershell
python data_generator.py --files 10
```
Generator options:
| Option | Default | Meaning |
|---|---|---|
| `--output-dir` | `data/incoming` | where CSV files are written |
| `--events-per-file` | 20 | rows per file |
| `--interval` | 5.0 | seconds between files |
| `--files` | 0 | number of files (0 = run until stopped) |
| `--bad-rate` | 0.05 | fraction of corrupted rows |

Terminal 1 prints one line per micro-batch:
`[batch N] received=... written=... rejected=... {reasons} took=...s rate=... rows/s`
Stop the stream with Ctrl+C (wait a few seconds after the generator finishes so the last batch is processed).

## 6. Check the results
```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d ecommerce_streaming -c "SELECT count(*) FROM events;"
```
The count should equal the sum of `written` in the log.

## 7. Reset between runs
```powershell
Remove-Item -Recurse -Force checkpoints -ErrorAction SilentlyContinue
Remove-Item data\incoming\* -Force -ErrorAction SilentlyContinue
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d ecommerce_streaming -c "TRUNCATE events;"
```
Always reset all three together. If the checkpoint is deleted but the table is not, Spark re-reads
old files and the primary key rejects the duplicates.

## 8. Troubleshooting
| Symptom | Cause / fix |
|---|---|
| `UnsatisfiedLinkError NativeIO` | Hadoop binaries or `HADOOP_HOME` missing (section 3) |
| Batch lines show tables, table stays empty | The console sink is still used; the stream must use `foreachBatch` |
| Duplicate key error on insert | Checkpoint reset without `TRUNCATE events` |
| `password authentication failed` | Wrong `PG_PASSWORD`, or the role was not created |
| "Current batch is falling behind" | Batch took longer than the 5 s trigger; normal for batch 0, otherwise reduce load |
| Stream does not pick up files | Files must land in `data/incoming`; names starting with `.` or `_` are ignored |
| Java processes remain after a crash | `Get-Process java`, then `Stop-Process -Name java` |
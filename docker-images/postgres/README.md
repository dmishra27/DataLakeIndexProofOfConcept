This folder contains the Docker build context for the local Postgres image.

Build it with:

```powershell
docker build -t datalake-postgres:17 .\docker-images\postgres
```

Run it with:

```powershell
Copy-Item .\.env.example .\.env
docker run --rm -p 5432:5432 --env-file .\.env datalake-postgres:17
```

An exported image tarball such as `postgres-17.tar` can still live here for local
convenience, but it is ignored by git.

# Kwil <> TimescaleDB

This repo is a basic example of how TimescaleDB can be used with Kwil. It uses [Kuneiform Precompiles](<https://docs.kwil.com/docs/extensions/precompiles>) to allow Kuneiform to have access to a Timescale database.

To run it, build a Postgres image using the Dockerfile and run Kwild:

```shell
docker build build/. -t kwil-pg-timescale
```

```shell
go run ./cmd --autogen
```

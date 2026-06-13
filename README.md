# Rustlog

## Description
Rustlog is a Twitch logging service based on [justlog](https://github.com/gempir/justlog). It provides the same web UI and API, but it utilizes [Clickhouse](https://clickhouse.com) for storage instead of text files.


## Installation

Create a `config.json` file (see [CONFIG.md](./docs/CONFIG.md))

### Docker
```yaml
version: "3.8"
  
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse
    volumes:
      - "./ch-data:/var/lib/clickhouse:rw"
    environment:
      CLICKHOUSE_DB: "rustlog"
      CLICKHOUSE_USER: "user"
      CLICKHOUSE_PASSWORD: "SuperSecretPassword"
    restart: unless-stopped
        
  rustlog:
    image: ghcr.io/boring-nick/rustlog:master
    container_name: rustlog
    ports:
      - 8025:8025 
    volumes:
      - "./config.json:/config.json"
    depends_on: 
      - clickhouse
    restart: unless-stopped
```

### From source

- Follow the [Contributing](Contributing) excluding the last step
- `cargo build --release`
- The resulting binary will be at `target/release/rustlog`

## Advantages over justlog

- Significantly better storage efficiency (3x+ improvement) thanks to not duplicating log files, more efficient structure and better compression (using ZSTD in Clickhouse)
- Blazing fast log queries with response streaming and a [highly performant IRC parser](https://github.com/jprochazk/twitch-rs)
- Support for ndjson logs responses

## Admin API

Rustlog exposes admin endpoints for managing the channels it logs. They are protected by the `adminAPIKey` set in your config file, which must be sent in the `X-Api-Key` header. If `adminAPIKey` is not configured, these endpoints are disabled.

Channels can be specified by Twitch user id (`channels`) or by login name (`channel_names`). Both fields are optional and may be combined in a single request. Login names are resolved to ids automatically. Unknown, banned, or nonexistent logins are skipped rather than causing an error.

### Add channels

```
POST /admin/channels
X-Api-Key: <adminAPIKey>
Content-Type: application/json

{ "channel_names": ["forsen", "xqc"], "channels": ["12345"] }
```

### Remove channels

```
DELETE /admin/channels
X-Api-Key: <adminAPIKey>
Content-Type: application/json

{ "channel_names": ["forsen", "xqc"] }
```

### Bulk import by username

To import many channels at once, send a single request with all the login names. The example below builds the request body from a newline-delimited file (`names.txt`) using `jq`.

```bash
jq -R -s '{channel_names: (split("\n") | map(select(length>0)))}' names.txt \
  | curl -X POST http://localhost:8025/admin/channels \
      -H "X-Api-Key: $ADMIN_API_KEY" \
      -H "Content-Type: application/json" \
      -d @-
```

Names are resolved against the Twitch Helix API in batches of 100. The channels themselves are joined over IRC sequentially, so a very large import will take some time to fully connect (subject to Twitch's join rate limits).

## Contributing

Requirements:
- rust
- yarn
- docker with docker-compose (optional, will need to set up Clickhouse manually without it)

Steps:

0. Clone the repository (make sure to include submodules!):
```
git clone --recursive https://github.com/boring-nick/rustlog
```
If you already cloned the repo without `--recursive`, you can initialize submodules with:
```
git submodule update --init --recursive
```

1. Set up the database (Clickhouse):

This repository provides a docker-compose to quickly set up Clickhouse. You can use it with:
```
docker-compose -f docker-compose.dev.yml up -d
```
Alternatively, you can install Clickhouse manually using the [official guide](https://clickhouse.com/docs/en/install).

2. Create a config file

Copy `config.dist.json` to `config.json` and configure your database and twitch credentials. If you installed Clickhouse with Docker, the default database configuration works.

3. Build the frontend:
```
cd web
yarn install
yarn build
cd ..
```
4. Build and run rustlog:
```
cargo run
```

You can now access rustlog at http://localhost:8025.

## Migrating from justlog
See [MIGRATION.md](./docs/MIGRATION.md)

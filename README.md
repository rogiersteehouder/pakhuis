# Pakhuis

## Table of Contents
+ [About](#about)
+ [Getting Started](#getting_started)
+ [API](#api)
+ [Notes](#notes)

## About <a name="about"></a>

Pakhuis is a webservice for storing json data with a limited search capability.

## Getting Started <a name="getting_started"></a>

### Prerequisites

+ Python 3.12 (may work with 3.11, no guarantees)
+ Python packages:
	+ httpx
	+ jsonpatch
	+ jsonpointer
	+ loguru
	+ starlette
	+ typer
	+ uvicorn

### Using Docker

This setup uses [Caddy](https://caddyserver.com/) to handle the external
connections, authentication and ssl/tls certificates. It will use two
Docker containers: one for Caddy from Docker Hub and a custom build for
Pakhuis itself.

Clone the repo (or download it).

```bash
$ git clone https://github.com/rogiersteehouder/pakhuis.git
```

Review the `compose.yaml` and `Dockerfile` files. Don't forget to set the
`PAKHUIS_VERSION` environment variable.

```bash
$ PAKHUIS_VERSION=$(git describe --tags)
$ docker compose build
$ docker compose up -d
```

And to update:

```bash
$ docker compose stop
$ git pull
$ PAKHUIS_VERSION=$(git describe --tags)
$ docker compose build
$ docker compose up -d
```

### Installing

Clone the repo (or download it).

```bash
$ git clone https://github.com/rogiersteehouder/pakhuis.git
```

Install the required python packages (in a virtual environment).

```bash
$ python3 -m venv .venv
$ .venv/bin/activate
$ python3 -m pip install -r requirements.txt
```

Copy and edit the config file. Use an instance directory to keep the working
files (config, logs and database) together.

```
$ mkdir instance
$ cp config-example.toml instance/config.toml
$ $EDITOR instance/config.toml
```

And start the server.

```
$ python -m pakhuis --cfg instance/config.toml
```

### Sync

You can keep two Pakhuis servers in sync using the sync function.

```
$ cp pakhuis-servers.example.toml pakhuis-servers.toml
$ $EDITOR pakhuis-servers.toml
```

```
$ python -m pakhuis.sync --cfg pakhuis-servers.toml
```

## API

Documentation unfinished. Sorry.

## Notes <a name="notes"></a>

I removed basic authentication. Use your own, or use a proxy like
[Caddy](https://caddyserver.com/).

# Pakhuis

## Table of Contents
+ [About](#about)
+ [Getting Started](#getting_started)
+ [Usage](#usage)
+ [API](#api)
+ [Notes](#notes)

## About <a name = "about"></a>

Pakhuis is a webservice for storing json data with a limited search capability.

## Getting Started <a name = "getting_started"></a>

### Prerequisites

+ Python 3.8+

### Installing

Clone the repo (or download it).

```
> git clone git@github.com:rogiersteehouder/pakhuis.git
```

Install the required python packages (in a virtual environment).

```
> python3 -m venv .venv
> .venv/bin/activate
> pip install -r requirements.txt
```

For python versions below 3.11, you also need the tomli package,

```
> pip install tomli
```

## Usage <a name="usage"></a>

Copy and edit the config file. Use an instance directory to keep the working
files (config, logs and database) together.

```
> mkdir instance
> cp config-example.toml instance/config.toml
> $EDITOR instance/config.toml
```

And start the server.

```
> python -m pakhuis --cfg instance/config.toml
```

### Sync

You can keep two Pakhuis servers in sync using the sync function.

```
> cp pakhuis-sync-exmple.toml pakhuis-sync.toml
> $EDITOR pakhuis-sync.toml
```

```
> python -m pakhuis.sync --cfg pakhuis-sync.toml
```

### API

Documentation unfinished. Sorry.

## Notes <a name="notes"></a>

Pakhuis uses Poorthuis for authentication (http basic authentication). This
will be unsafe unless you use SSL/TLS (see config file on how to specify ssl
certificate files).

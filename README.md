# TinyLSM

- [English Document](README.md)
- [中文文档](README-CN.md)  

Tiny LSM in scala, [Reference to LSM in a Week](https://skyzh.github.io/mini-lsm/00-preface.html)

## Requirement

development:

- JDK 11 or later
- sbt

build:

- Docker

## Configuration

Configuration lookup order:

1. JVM system properties, e.g., `-Dkey.subkey=value`
2. Operation system environment e.g., `export TINY_LSM_KEY_SUBKEY=value`
3. `.env` file in classpath, using operation system environment key, e.g., `TINY_LSM_KEY_SUBKEY=value`
4. config file specified by JVM system properties `config.file` or operation system environment `TINY_LSM_CONFIG_FILE`; this config file uses JVM system properties key, e.g., `key.subkey=value`

| environment key               | system properties name | meaning                                                                      | default value             |
|-------------------------------|------------------------|------------------------------------------------------------------------------|---------------------------|
| TINY_LSM_PORT                 | port                   |                                                                              | 9527                      |
| TINY_LSM_LISTEN               | listen                 |                                                                              | 0.0.0.0                   |
| TINY_LSM_BLOCK_SIZE           | block.size             | Block size in bytes                                                          | 4096                      |
| TINY_LSM_TARGET_SST_SIZE      | target.sst.size        | SST size in bytes, also the approximate memtable capacity limit              | 2 << 20 (2MB)             |
| TINY_LSM_TARGET_MANIFEST_SIZE | target.manifest.size   | Manifest size in bytes                                                       | 1 << 20 (1MB)             |
| TINY_LSM_MEMTABLE_NUM         | memtable.num           | Maximum number of memtables in memory, flush to L0 when exceeding this limit | 50                        |
| TINY_LSM_ENABLE_WAL           | enable.wal             |                                                                              | true                      |
| TINY_LSM_SERIALIZABLE         | serializable           | Transaction serializable                                                     | false                     |
| TINY_LSM_DATA_DIR             | data.dir               |                                                                              | /etc/tinylsm/data         |
| TINY_LSM_CONFIG_FILE          | config.file            |                                                                              | /etc/tinylsm/tinylsm.conf |
| TINY_LSM_COMPACTION_STRATEGY  | compaction.strategy    | leveled/tiered/simple/full/none                                              | leveled                   |

Compaction strategy config detail as below.

Leveled compaction configs:

| environment key                             | system properties name             | meaning | default |
|---------------------------------------------|------------------------------------|---------|---------|
| TINY_LSM_COMPACTION_LEVEL_SIZE_MULTIPLIER   | compaction.level.size.multiplier   |         | 4       |
| TINY_LSM_COMPACTION_LEVEL0_FILE_NUM_TRIGGER | compaction.level0.file.num.trigger |         | 5       |
| TINY_LSM_COMPACTION_MAX_LEVELS              | compaction.max.levels              |         | 5       |
| TINY_LSM_COMPACTION_BASE_LEVEL_SIZE_MB      | compaction.base.level.size.mb      |         | 100     |

Tiered compaction configs:

| environment key                          | system properties name          | meaning | default |
|------------------------------------------|---------------------------------|---------|---------|
| TINY_LSM_COMPACTION_MAX_SIZE_AMP_PERCENT | compaction.max.size.amp.percent |         | 200     |
| TINY_LSM_COMPACTION_SIZE_RATIO_PERCENT   | compaction.size.ratio.percent   |         | 200     |
| TINY_LSM_COMPACTION_MIN_MERGE_WIDTH      | compaction.min.merge.width      |         | 2       |
| TINY_LSM_COMPACTION_MAX_LEVELS           | compaction.max.levels           |         | 5       |

Simple compaction configs:

| environment key                             | system properties name             | meaning | default |
|---------------------------------------------|------------------------------------|---------|---------|
| TINY_LSM_COMPACTION_SIZE_RATIO_PERCENT      | compaction.size.ratio.percent      |         | 200     |
| TINY_LSM_COMPACTION_LEVEL0_FILE_NUM_TRIGGER | compaction.level0.file.num.trigger |         | 5       |
| TINY_LSM_COMPACTION_MAX_LEVELS              | compaction.max.levels              |         | 5       |

For example, write a config file in `/path/to/tinylsm.conf` :

```properties
port=9527
listen=0.0.0.0
block.size=4096
target.sst.size=2097152
memtable.num=50
enable.wal=true
serializable=false
data.dir=/etc/tinylsm/data
compaction.strategy=leveled
compaction.level.size.multiplier=5
compaction.level0.file.num.trigger=5
compaction.max.levels=5
base.level.size.mb=100
```

then `export TINY_LSM_CONFIG_FILE=/path/to/tinylsm.conf`( or `-e TINY_LSM_CONFIG_FILE=/path/to/tinylsm.conf` for docker
command) and start TinyLsm.

## Usage

### Get Docker Image

You can pull image from [docker hub](https://hub.docker.com/r/leibniz007/tiny-lsm/tags):

```bash
docker pull leibniz007/tiny-lsm:latest
```

Or build docker image from source code.  
Make sure `docker` is installed, and then execute:

```shell
docker build . -f Dockerfile -t leibniz007/tiny-lsm:latest --network=host --target prod
```

### Run

```shell
docker run --rm -d --name tinylsm -v /path/to/tinylsm:/etc/tinylsm leibniz007/tiny-lsm:latest
docker exec -it tinylsm bash

# in container's bash
tinylsm-cli

# in tinylsm-cli
:help
put key value
get key
delete key
get key
:quit
```

## TODO

- [ ] Benchmarking
- [ ] Block Compression
- [ ] Trivial Move and Parallel Compaction
- [ ] Alternative Block Encodings
- [ ] Rate Limiter and I/O Optimizations
- [ ] Async Engine
- [ ] IO-uring-based I/O engine
- [ ] Prefetching
- [ ] Key-Value Separation
- [ ] Column Families
- [ ] Sharding
- [ ] Compaction Optimizations
- [ ] SQL over Mini-LSM
# TinyLSM

Tiny LSM in scala

## Requirement

- JDK 11 or later

## Configuration

Configuration lookup order:

1. JVM system properties, e.g., `-Dkey.subkey=value`
2. Operation system environment e.g., `export TINY_LSM_KEY_SUBKEY=value`
3. `.env` file in classpath, using operation system environment key
4. config file specified by JVM system properties `config.file` or operation system environment `TINY_LSM_CONFIG_FILE`,
   using JVM system properties key

| environment key          | system properties name | meaning                                                                      | default value             |
|--------------------------|------------------------|------------------------------------------------------------------------------|---------------------------|
| TINY_LSM_PORT            | port                   |                                                                              | 9527                      |
| TINY_LSM_LISTEN          | listen                 |                                                                              | 0.0.0.0                   |
| TINY_LSM_BLOCK_SIZE      | block.size             | Block size in bytes                                                          | 4096                      |
| TINY_LSM_TARGET_SST_SIZE | block.size             | SST size in bytes, also the approximate memtable capacity limit              | 2 << 20 (2MB)             |
| TINY_LSM_MEMTABLE_NUM    | memtable.num           | Maximum number of memtables in memory, flush to L0 when exceeding this limit | 50                        |
| TINY_LSM_ENABLE_WAL      | enable.wal             |                                                                              | true                      |
| TINY_LSM_SERIALIZABLE    | serializable           |                                                                              | false                     |
| TINY_LSM_DATA_DIR        | data.dir               |                                                                              | /etc/tinylsm/data         |
| TINY_LSM_CONFIG_FILE     | config.file            |                                                                              | /etc/tinylsm/tinylsm.conf |

[Reference](https://skyzh.github.io/mini-lsm/00-preface.html)
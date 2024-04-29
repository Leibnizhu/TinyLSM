# TinyLSM

[![Docker Images](https://img.shields.io/docker/v/leibniz007/tinylsm?label=Docker&style=flat)](https://hub.docker.com/r/leibniz007/tinylsm/tags) ![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/Leibnizhu/TinyLSM/scala.yml) ![GitHub top language](https://img.shields.io/github/languages/top/Leibnizhu/TinyLSM) ![Github Created At](https://img.shields.io/github/created-at/Leibnizhu/TinyLSM)

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
4. config file specified by JVM system properties `config.file` or operation system environment `TINY_LSM_CONFIG_FILE`;
   this config file uses JVM system properties key, e.g., `key.subkey=value`

| environment key               | system properties name | meaning                                                                      | default value             |
|-------------------------------|------------------------|------------------------------------------------------------------------------|---------------------------|
| TINY_LSM_HTTP_PORT            | http.port              |                                                                              | 9527                      |
| TINY_LSM_GRPC_PORT            | grpc.port              |                                                                              | 9526                      |
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
| TINY_LSM_COMPRESSOR_TYPE      | compressor.type        | Value storage compression, none/no/zstd/zlib/lz4                             | zstd                      |

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

Value storage compression config detail as below.

Zstd compression configs:

| environment key           | system properties name | meaning | default      |
|---------------------------|------------------------|---------|--------------|
| TINY_LSM_ZSTD_TRAIN_DICT  | zstd.train.dict        |         | true         |
| TINY_LSM_ZSTD_SAMPLE_SIZE | zstd.sample.size       |         | 1048576(1MB) |
| TINY_LSM_ZSTD_DICT_SIZE   | zstd.dict.size         |         | 16384(16KB)  |
| TINY_LSM_ZSTD_LEVEL       | zstd.level             | 1-22    | 3            |

Zlib compression configs:

| environment key     | system properties name | meaning           | default |
|---------------------|------------------------|-------------------|---------|
| TINY_LSM_ZLIB_LEVEL | zlib.level             | -1 = default, 1-9 | -1      |

LZ4 compression configs:

| environment key    | system properties name | meaning                | default |
|--------------------|------------------------|------------------------|---------|
| TINY_LSM_LZ4_LEVEL | lz4.level              | -1 = fast, 1-17 = high | -1      |

For example, write a config file in `/path/to/tinylsm.conf` :

```properties
http.port=9527
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
compressor.type=zstd
zstd.sample.size=1048576
zstd.dict.size=16384
```

then `export TINY_LSM_CONFIG_FILE=/path/to/tinylsm.conf`( or `-e TINY_LSM_CONFIG_FILE=/path/to/tinylsm.conf` for docker
command) and start TinyLsm.

## Usage

### Get Docker Image

You can pull image from [docker hub](https://hub.docker.com/r/leibniz007/tinylsm/tags):

```bash
docker pull leibniz007/tinylsm:latest
```

Or build docker image from source code.  
Make sure `docker` is installed, and then execute:

```shell
docker build . -f Dockerfile -t leibniz007/tinylsm:latest --network=host --target prod
```

### Run

Create a dir for tinylsm first: 

```shell
mkdir tinylsm
```

And edit `tinylsm/tinylsm.conf` by yourself. Then start Docker container: 

```bash
docker run --rm -d --name tinylsm -v $(pwd)/tinylsm:/etc/tinylsm -p 9527:9527 -p 9526:9526 leibniz007/tinylsm:latest
```

### Command Line Tool(CLI)

In the docker image, we provided `tinylsm-cli` for connecting TinyLSM: 

```
docker exec -it tinylsm bash

# in container's bash
tinylsm-cli --help
tinylsm-cli

# in tinylsm-cli
:help
put key value
get key
delete key
get key
:quit
```

### gRPC

gRPC port is defined by enviroment `TINY_LSM_GRPC_PORT` or config property `grpc.port`, default port is `9526`.

gRPC's definition refers to [tinylsm.proto](src/main/protobuf/tinylsm.proto).

you can use gRPC clients like [evans](https://github.com/ktr0731/evans) or java/scala code to connect to gRPC server.
Sample code as below:

```scala
package io.github.leibnizhu.tinylsm.grpc

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.grpc.GrpcClientSettings
import com.google.protobuf.ByteString
import io.github.leibnizhu.tinylsm.grpc.*

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

object GprcClientSample {
   implicit val sys: ActorSystem[Nothing] = ActorSystem[Nothing](Behaviors.empty[Nothing], "TinyLsmClient")
   implicit val ec: ExecutionContext = sys.executionContext
   private val grpcClient = TinyLsmRpcServiceClient(
      GrpcClientSettings.connectToServiceAt("localhost", 9526).withTls(false))

   private def getByKeyTest(): Unit = {
      val msg = Await.result(grpcClient.getKey(GetKeyRequest(ByteString.copyFromUtf8("testKey")))
         , 5.second)
      println("getKey result:" + msg.value.toStringUtf8)
      assert("testValue" == msg.value.toStringUtf8)
   }

   private def putValueTest(): Unit = {
      val reply = Await.result(grpcClient.putKey(PutKeyRequest(
         ByteString.copyFromUtf8("testKey"),
         ByteString.copyFromUtf8("testValue"))), 5.second)
      println("putKey success")
   }

   def main(args: Array[String]): Unit = {
      putValueTest()
      getByKeyTest()
   }
}
```

### Http API

http port is defined by enviroment `TINY_LSM_HTTP_PORT` or config property `http.port`, default port is `9527`.

| URL                     | parameter                                                                                      | usage                                |
|-------------------------|------------------------------------------------------------------------------------------------|--------------------------------------|
| GET /key/$key           | `tid`=Transaction ID                                                                           | get value by key                     |
| POST /key/$key          | `tid`=Transaction ID, `value`=value                                                            | put value by key                     |
| DELETE /key/$key        | `tid`=Transaction ID                                                                           | delete a key                         |
| POST /scan              | `tid`=Transaction ID, `fromType`,`fromKey`,`toType`,`toKey`, type: unbounded/included/excluded | scan key-value by key range          |
| POST /sys/flush         |                                                                                                | force flush memtable                 |
| POST /sys/state         |                                                                                                | dump storage structure               |
| POST /txn               |                                                                                                | start a new Transaction,return `tid` |
| POST /txn/$tid/commit   |                                                                                                | commit a Transaction                 |
| POST /txn/$tid/rollback |                                                                                                | rollback a new Transaction           |_

## BenchMark

Apple M1 Pro 8 core

| Item                   | result               |
|------------------------|----------------------|
| get 10k keys           | 1.161 ± 0.792   s/op |
| get 10k keys with zstd | 1.023 ± 0.150   s/op |
| get 10k keys with zlib | 1.081 ± 0.238   s/op |
| put 10k keys           | 0.035 ± 0.458   s/op |

Equivalent to:

1. `get` one key costs 116.1 ± 79.2 us
2. `put` one key costs 3.5 ± 45.8 us

## TODO

- [x] Benchmarking
- [x] Block Compression
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
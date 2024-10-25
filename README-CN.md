# TinyLSM

[![Docker](https://img.shields.io/docker/v/leibniz007/tinylsm?label=Docker&style=flat)](https://hub.docker.com/r/leibniz007/tinylsm/tags) ![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/Leibnizhu/TinyLSM/scala.yml) ![GitHub top language](https://img.shields.io/github/languages/top/Leibnizhu/TinyLSM) ![Github Created At](https://img.shields.io/github/created-at/Leibnizhu/TinyLSM) [![codecov](https://codecov.io/gh/Leibnizhu/TinyLSM/graph/badge.svg?token=9MBLQB1I6R)](https://codecov.io/gh/Leibnizhu/TinyLSM)

- [English Document](README.md)
- [中文文档](README-CN.md)

使用Scala开发的羽量级LSM k-v 存储, [可参考 LSM in a Week](https://skyzh.github.io/mini-lsm/00-preface.html)

## 环境要求

开发环境:

- JDK 11 或更高版本
- sbt

编译环境:

- Docker

## 配置

配置优先级从高到低:

1. JVM系统属性，通过JVM的 `-Dkey.subkey=value` 启动参数来配置
2. 操作系统的环境变量，通过 `export TINY_LSM_KEY_SUBKEY=value` 命令来配置
3. classpath中的 `.env` 文件，配置名与系统环境变量一致，如 `TINY_LSM_KEY_SUBKEY=value`
4. 通过JVM 系统属性 `config.file` 或操作系统的环境变量 `TINY_LSM_CONFIG_FILE`
   指定的配置文件路径，配置文件中的配置名与JVM系统属性一致，如 `key.subkey=value`

| 环境变量配置名                       | 系统属性配置名              | 含义                                  | 默认值                       |
|-------------------------------|----------------------|-------------------------------------|---------------------------|
| TINY_LSM_HTTP_PORT            | http.port            |                                     | 9527                      |
| TINY_LSM_GRPC_PORT            | grpc.port            |                                     | 9526                      |
| TINY_LSM_LISTEN               | listen               |                                     | 0.0.0.0                   |
| TINY_LSM_BLOCK_SIZE           | block.size           | SST的Block大小阈值，单位为byte               | 4096                      |
| TINY_LSM_TARGET_SST_SIZE      | target.sst.size      | SST大小阈值，单位为byte，同时用于MemTable预估大小的阈值 | 2 << 20 (2MB)             |
| TINY_LSM_TARGET_MANIFEST_SIZE | target.manifest.size | Manifest文件大小阈值，单位为byte              | 1 << 20 (1MB)             |
| TINY_LSM_MEMTABLE_NUM         | memtable.num         | 内存中MemTable数量上限，超过这个阈值后flush到L0     | 50                        |
| TINY_LSM_ENABLE_WAL           | enable.wal           |                                     | true                      |
| TINY_LSM_SERIALIZABLE         | serializable         | 事务是否可线性化                            | false                     |
| TINY_LSM_DATA_DIR             | data.dir             |                                     | /etc/tinylsm/data         |
| TINY_LSM_CONFIG_FILE          | config.file          |                                     | /etc/tinylsm/tinylsm.conf |
| TINY_LSM_COMPACTION_STRATEGY  | compaction.strategy  | leveled/tiered/simple/full/none     | leveled                   |
| TINY_LSM_COMPRESSOR_TYPE      | compressor.type      | value存储压缩算法, none/no/zstd/zlib/lz4  | zstd                      |

SST压缩（compaction）的详细配置如下。

Leveled compaction 配置：

| 环境变量配置名                                     | 系统属性配置名                            | 含义 | 默认值 |
|---------------------------------------------|------------------------------------|----|-----|
| TINY_LSM_COMPACTION_LEVEL_SIZE_MULTIPLIER   | compaction.level.size.multiplier   |    | 4   |
| TINY_LSM_COMPACTION_LEVEL0_FILE_NUM_TRIGGER | compaction.level0.file.num.trigger |    | 5   |
| TINY_LSM_COMPACTION_MAX_LEVELS              | compaction.max.levels              |    | 5   |
| TINY_LSM_COMPACTION_BASE_LEVEL_SIZE_MB      | compaction.base.level.size.mb      |    | 100 |

Tiered compaction 配置：

| 环境变量配置名                                  | 系统属性配置名                         | 含义 | 默认值 |
|------------------------------------------|---------------------------------|----|-----|
| TINY_LSM_COMPACTION_MAX_SIZE_AMP_PERCENT | compaction.max.size.amp.percent |    | 200 |
| TINY_LSM_COMPACTION_SIZE_RATIO_PERCENT   | compaction.size.ratio.percent   |    | 200 |
| TINY_LSM_COMPACTION_MIN_MERGE_WIDTH      | compaction.min.merge.width      |    | 2   |
| TINY_LSM_COMPACTION_MAX_LEVELS           | compaction.max.levels           |    | 5   |

Simple compaction 配置：

| 环境变量配置名                                     | 系统属性配置名                            | 含义 | 默认值 |
|---------------------------------------------|------------------------------------|----|-----|
| TINY_LSM_COMPACTION_SIZE_RATIO_PERCENT      | compaction.size.ratio.percent      |    | 200 |
| TINY_LSM_COMPACTION_LEVEL0_FILE_NUM_TRIGGER | compaction.level0.file.num.trigger |    | 5   |
| TINY_LSM_COMPACTION_MAX_LEVELS              | compaction.max.levels              |    | 5   |

Value存储压缩（compression）的详细配置如下。

`Zstd` 压缩配置：

| 环境变量配置名                   | 系统属性配置名          | 含义   | 默认值          |
|---------------------------|------------------|------|--------------|
| TINY_LSM_ZSTD_TRAIN_DICT  | zstd.train.dict  |      | true         |
| TINY_LSM_ZSTD_SAMPLE_SIZE | zstd.sample.size |      | 1048576(1MB) |
| TINY_LSM_ZSTD_DICT_SIZE   | zstd.dict.size   |      | 16384(16KB)  |
| TINY_LSM_ZSTD_LEVEL       | zstd.level       | 1-22 | 3            |

`Zlib` 压缩配置：

| environment key     | system properties name | meaning          | default |
|---------------------|------------------------|------------------|---------|
| TINY_LSM_ZLIB_LEVEL | zlib.level             | 压缩级别，-1=默认, 取1-9 | -1      |

`LZ4` 压缩配置：

| environment key    | system properties name | meaning              | default |
|--------------------|------------------------|----------------------|---------|
| TINY_LSM_LZ4_LEVEL | lz4.level              | -1 = 快速, 1-17 = 压缩级别 | -1      |

举例，可编写一个配置文件 `/path/to/tinylsm.conf` :

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
zstd.train.dict=true
zstd.sample.size=1048576
zstd.dict.size=16384
```

然后执行 `export TINY_LSM_CONFIG_FILE=/path/to/tinylsm.conf` (
对于Docker运行则为增加 `-e TINY_LSM_CONFIG_FILE=/path/to/tinylsm.conf` 参数） 再启动 TinyLSM。

## 使用方法

### 获取 Docker Image

可以从 [docker hub](https://hub.docker.com/r/leibniz007/tinylsm/tags) 拉取镜像:

```bash
docker pull leibniz007/tinylsm:latest
```

或使用源码编译Docker镜像。请先确保 `docker` 已安装，然后执行：

```shell
docker build . -f Dockerfile -t leibniz007/tinylsm:latest --network=host --target prod
```

### 运行

创建目录：

```shell
mkdir tinylsm
```

参照前面章节编辑 tinylsm/tinylsm.conf 配置文件。然后启动Docker容器

```bash
docker run --rm -d --name tinylsm -v $(pwd)/tinylsm:/etc/tinylsm -p 9527:9527 -p 9526:9526 leibniz007/tinylsm:latest
```

此时可使用 9526 端口的gRPC服务，或 9527 端口的http服务。

### 命令行工具(CLI)

我们在镜像中提供了 `tinylsm-cli` 命令用于通过gRPC连接TinyLSM服务。

```
docker exec -it tinylsm bash

# 以下是在Docker容器的 bash 中
tinylsm-cli --help
tinylsm-cli

# 以下是在 tinylsm-cli 中
:help
put key value
get key
delete key
get key
:quit
```

### gRPC

gRPC端口由环境变量 `TINY_LSM_GRPC_PORT` 或属性 `grpc.port` 配置，默认为 `9526`。

gRPC的定义参见 [tinylsm.proto](src/main/protobuf/tinylsm.proto)。

可以使用gRPC客户端、如 [evans](https://github.com/ktr0731/evans) 等，或 java/scala 代码去连接gRPC服务。

以下是使用scala连接的样例代码：

```scala
package io.github.leibnizhu.tinylsm.grpc

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.grpc.GrpcClientSettings
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
      sys.terminate()
   }
}
```

### Http API

http端口由环境变量 `TINY_LSM_HTTP_PORT` 或属性 `http.port` 配置，默认为 `9527`.

| URL                     | 参数                                                                                    | 使用说明               |
|-------------------------|---------------------------------------------------------------------------------------|--------------------|
| GET /key/$key           | `tid`=事务ID                                                                            | 获取指定key的值          |
| POST /key/$key          | `tid`=事务ID, `value`=value                                                             | 更新指定key的值          |
| DELETE /key/$key        | `tid`=事务ID                                                                            | 删除指定key            |
| POST /scan              | `tid`=事务ID, `fromType`,`fromKey`,`toType`,`toKey`, type取值：unbounded/included/excluded | 按key范围扫描           |
| POST /sys/flush         |                                                                                       | 强制flush memtable   |
| POST /sys/state         |                                                                                       | 打印当前存储结构           |
| POST /txn               |                                                                                       | 开启新事务，返回事务ID `tid` |
| POST /txn/$tid/commit   |                                                                                       | 提交事务               |
| POST /txn/$tid/rollback |                                                                                       | 回滚事务               |

## BenchMark

Apple M1 Pro 8 core

| 项目                     | 结果                   |
|------------------------|----------------------|
| get 10k keys           | 1.161 ± 0.792   s/op |
| get 10k keys with zstd | 1.023 ± 0.150   s/op |
| get 10k keys with zlib | 1.081 ± 0.238   s/op |
| put 10k keys           | 0.035 ± 0.458   s/op |

等价于:

1. `get` 一个key要 116.1 ± 79.2 us
2. `put` 一个key要 3.5 ± 45.8 us

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
package io.github.leibnizhu.tinylsm.block

import com.github.blemale.scaffeine.{Cache, Scaffeine}

import scala.concurrent.duration.*


type BlockCache = Cache[(Int, Int), Block]

object BlockCache {

  def apply(maxSize: Int, expire: FiniteDuration = 10.minute): BlockCache = {
    Scaffeine()
      .recordStats()
      .expireAfterWrite(expire)
      .maximumSize(maxSize)
      .build()
  }
}
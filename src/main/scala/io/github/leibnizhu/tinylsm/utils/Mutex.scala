package io.github.leibnizhu.tinylsm.utils

import io.github.leibnizhu.tinylsm.LsmStorageState

import java.util.concurrent.locks.ReentrantLock

case class Mutex[T](private var inner: T) {
  private val lock = new ReentrantLock()

  def execute[R](f: T => R): R = {
    try {
      lock.lock()
      f(this.inner)
    } finally {
      lock.unlock()
    }
  }

  def update(newInner: T => T): Mutex[T] = {
    try {
      lock.lock()
      this.inner = newInner(this.inner)
    } finally {
      lock.unlock()
    }
    this
  }
}

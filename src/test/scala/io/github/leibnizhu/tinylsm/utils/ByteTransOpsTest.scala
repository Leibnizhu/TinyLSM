package io.github.leibnizhu.tinylsm.utils

import org.scalatest.funsuite.AnyFunSuite

class ByteTransOpsTest extends AnyFunSuite {

  test("intLow2Bytes") {
    assertResult(List(2, 1))(ByteTransOps.intLow2Bytes(513))
  }

  test("low2BytesToInt") {
    assertResult(513)(ByteTransOps.low2BytesToInt(2, 1))
  }

  test("arrayPrefixes") {
    assertResult(Array("aaa", "aaa:bbb"))(ByteTransOps.arrayPrefixes("aaa:bbb:ccc".getBytes, ':').map(new String(_)))
    assertResult(Array("aaa", "aaa:bbb", "aaa:bbb:cc"))(ByteTransOps.arrayPrefixes("aaa:bbb:cc:".getBytes, ':').map(new String(_)))
  }
}

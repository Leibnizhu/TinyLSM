package io.github.leibnizhu.tinylsm

import org.mockito.Mockito.{mock, when}
import org.scalatest.Entry
import org.scalatest.funsuite.AnyFunSuite

class FusedIteratorTest extends AnyFunSuite {

  test("week1_day2_task3_fused_iterator") {
    val fusedIter1 = new FusedIterator(MemTableIterator(Iterator.empty))
    assert(!fusedIter1.isValid)
    fusedIter1.next()
    fusedIter1.next()
    fusedIter1.next()
    assert(!fusedIter1.isValid)


    val errIter = mock(classOf[Iterator[MemTableEntry]])
    when(errIter.hasNext).thenReturn(true).thenReturn(true).thenReturn(false)
    when(errIter.next()).thenReturn(entry("a", "1.1")).thenThrow(new RuntimeException())
    val fusedIter2 = new FusedIterator(MemTableIterator(errIter))
    assert(fusedIter2.isValid)
    assertThrows[Exception](fusedIter2.next())
    assert(!fusedIter2.isValid)
    assertThrows[Exception](fusedIter2.next())
    assertThrows[Exception](fusedIter2.next())
  }

  private def entry(k: String, v: String): MemTableEntry = {
    Entry(ByteArrayKey(k.getBytes), v.getBytes)
  }
}

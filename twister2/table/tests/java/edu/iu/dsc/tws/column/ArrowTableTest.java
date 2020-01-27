package edu.iu.dsc.tws.column;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.Test;

public class ArrowTableTest {

  @Test
  public void testPacking() {
    IntVector vector = new IntVector("first", new RootAllocator());
    for (int i = 0; i < 100; i++) {
      vector.set(i, i + 1);
    }
  }
}
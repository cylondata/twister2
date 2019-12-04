package edu.iu.dsc.tws.python.tset;

import java.util.List;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.python.processors.PythonClassProcessor;

public class PyTSetKeyedSource implements SourceFunc {

  private PythonClassProcessor processor;

  public PyTSetKeyedSource(byte[] lambda) {
    this.processor = new PythonClassProcessor(lambda);
  }

  @Override
  public boolean hasNext() {
    return (Boolean) this.processor.invoke("has_next");
  }

  @Override
  public Object next() {
    List next = (List) this.processor.invoke("next");
    return Tuple.of(next.get(0), next.get(1));
  }
}

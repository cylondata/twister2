package edu.iu.dsc.tws.python.processors;

import java.util.Map;

public class PythonClassProcessor extends AbstractPythonProcessor {

  public PythonClassProcessor(byte[] bytes) {
    super(bytes);
  }

  public Object invoke(String func, Object... args) {
    return super.invoke(this.objectId + "." + func, getEmptyKwargs(), args);
  }

  public Object invoke(String func, Map<String, Object> kwargs, Object... args) {
    return super.invoke(this.objectId + "." + func, kwargs, args);
  }
}

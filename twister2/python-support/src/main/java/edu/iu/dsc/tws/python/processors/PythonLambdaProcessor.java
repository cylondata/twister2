package edu.iu.dsc.tws.python.processors;

import java.util.Map;

public class PythonLambdaProcessor extends AbstractPythonProcessor {

  public PythonLambdaProcessor(byte[] lambda) {
    super(lambda);
  }

  public Object invoke(Object args) {
    return super.invoke(this.objectId, getEmptyKwargs(), args);
  }

  public Object invoke(Object arg1, Object arg2) {
    return super.invoke(this.objectId, getEmptyKwargs(), arg1, arg2);
  }

  public Object invoke(Object args, Map<String, Object> kwargs) {
    return super.invoke(this.objectId, kwargs, args);
  }
}

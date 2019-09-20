package edu.iu.dsc.tws.python.processors;

public class PythonLambdaProcessor extends AbstractPythonProcessor {

  public PythonLambdaProcessor(byte[] lambda) {
    super(lambda);
  }

  public Object invoke(Object args) {
    return super.invoke(this.objectId, args);
  }

  public Object invoke(Object arg1, Object arg2) {
    return super.invoke(this.objectId, arg1, arg2);
  }
}

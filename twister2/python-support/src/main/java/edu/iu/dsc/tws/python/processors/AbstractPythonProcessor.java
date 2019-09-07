package edu.iu.dsc.tws.python.processors;

import java.io.Serializable;
import java.util.Base64;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

import jep.Jep;
import jep.JepException;

public abstract class AbstractPythonProcessor extends ThreadLocal<Jep> implements Serializable {

  private static int objectIdCounter = 0;
  protected final String objectId;
  private byte[] bytes;

  @Override
  protected Jep initialValue() {
    try {
      //get the jep instance for this thread
      Jep jep = JepInstance.getInstance();

      // todo temporary workaround for JepArray issue.
      // This won't be a significant performance bottleneck though
      String lambdaString = Base64.getEncoder().encodeToString(bytes);
      jep.set("func_bin", lambdaString);
      jep.eval(this.objectId + " = cp.loads(base64.b64decode(func_bin))");
      jep.eval("del func_bin");
      return jep;
    } catch (JepException e) {
      throw new Twister2RuntimeException("Error in building lambda function", e);
    }
  }

  AbstractPythonProcessor(byte[] bytes) {
    this.objectId = "lambda_func_" + objectIdCounter++;
    this.bytes = bytes;
  }

  Object invoke(String handler, Object... args) {
    try {
      return this.get().invoke(handler, args);
    } catch (JepException e) {
      throw new Twister2RuntimeException("Error in invoking python function", e);
    }
  }
}

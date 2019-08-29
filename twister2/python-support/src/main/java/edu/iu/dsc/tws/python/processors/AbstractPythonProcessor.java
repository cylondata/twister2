package edu.iu.dsc.tws.python.processors;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import jep.Jep;
import jep.JepException;

import java.util.Base64;

public abstract class AbstractPythonProcessor {

    private static int objectIdCounter = 0;
    final String objectId;

    AbstractPythonProcessor(byte[] bytes) {
        this.objectId = "lambda_func_" + objectIdCounter++;
        try {
            Jep jep = JepInstance.get();

            // todo temporary workaround for JepArray issue.
            // This won't be a significant performance bottleneck though
            String lambdaString = Base64.getEncoder().encodeToString(bytes);
            jep.set("func_bin", lambdaString);
            jep.eval(this.objectId + " = cp.loads(base64.b64decode(func_bin))");
            jep.eval("del func_bin");
        } catch (JepException e) {
            throw new Twister2RuntimeException("Error in building lambda function", e);
        }
    }

    Object invoke(String handler, Object... args) {
        try {
            return JepInstance.get().invoke(handler, args);
        } catch (JepException e) {
            throw new Twister2RuntimeException("Error in invoking python function", e);
        }
    }
}

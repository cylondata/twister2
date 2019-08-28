package edu.iu.dsc.tws.python.processors;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import jep.Jep;
import jep.JepException;

import java.util.Base64;

public class PythonLambdaProcessor {

    private Jep jep;

    public PythonLambdaProcessor(byte[] lambda) {
        try {
            this.jep = new Jep();
            this.jep.eval("import cloudpickle as cp");
            this.jep.eval("import base64");

            // todo temporary workaround for JepArray issue.
            // This won't be a significant performance bottleneck though
            String lambdaString = Base64.getEncoder().encodeToString(lambda);
            this.jep.set("func_bin", lambdaString);
            this.jep.eval("func = cp.loads(base64.b64decode(func_bin))");
            //this.jep.eval("print(func)");
        } catch (JepException e) {
            throw new Twister2RuntimeException("Error in building lambda function", e);
        }
    }

    public Object process(Object... args) {
        try {
            return this.jep.invoke("func", args);
        } catch (JepException e) {
            throw new Twister2RuntimeException("Error in invoking python lambda function", e);
        }
    }
}
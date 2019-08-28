package edu.iu.dsc.tws.python.processors;

import jep.Jep;
import jep.JepException;

public class JepInstance {

    private static volatile Jep jep;

    public static Jep get() throws JepException {
        if (jep == null) {
            jep = new Jep();
        }
        return jep;
    }
}

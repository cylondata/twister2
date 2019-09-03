package edu.iu.dsc.tws.python.processors;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;

public final class JepInstance {

  private static volatile Jep jep;

  private JepInstance() {

  }

  public static Jep get() throws JepException {
    if (jep == null) {
      JepConfig jepConfig = new JepConfig();
      //todo temp fix
      jepConfig.addIncludePaths("/home/chathura/Code/twister2/twister2/python-support/src/main/python");
      jep = new Jep(jepConfig);
      jep.eval("import cloudpickle as cp");
      jep.eval("import base64");
      jep.eval("from twister2.tset.fn.SourceFunc import SourceFunc");
      jep.eval("from abc import ABC, abstractmethod");
    }
    return jep;
  }
}

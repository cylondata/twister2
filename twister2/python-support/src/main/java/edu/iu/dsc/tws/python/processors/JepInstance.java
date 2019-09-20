package edu.iu.dsc.tws.python.processors;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;

public final class JepInstance extends ThreadLocal<Jep> {

  private static JepInstance jepInstance = new JepInstance();

  //private static volatile Jep jep;
  private JepInstance() {

  }

  public static Jep getInstance() {
    return jepInstance.get();
  }

  @Override
  protected Jep initialValue() {
    JepConfig jepConfig = new JepConfig();
    jepConfig.setRedirectOutputStreams(true);
//    jepConfig.addIncludePaths(
//        "/home/chathura/Code/twister2/twister2/python-support/src/main/python"
//    );
    try {
      TSharedInterpreter jep = new TSharedInterpreter(jepConfig);
      jep.eval("import cloudpickle as cp");
      jep.eval("import base64");
      jep.eval("from abc import ABC, abstractmethod");
      jep.eval("import numpy as np");
      jep.eval("import twister2.utils as utils");

      return jep;
    } catch (JepException jex) {
      throw new Twister2RuntimeException("Couldn't create a JEP instance", jex);
    }
  }
}

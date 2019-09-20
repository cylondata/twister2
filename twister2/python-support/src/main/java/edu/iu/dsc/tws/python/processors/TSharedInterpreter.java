package edu.iu.dsc.tws.python.processors;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;

/**
 * We need to use {@link jep.SharedInterpreter} to handle numpy. But this class doesn't accept
 * {@link JepConfig} making us unable to redirect output stream. {@link TSharedInterpreter} support
 * both these features.
 */
public class TSharedInterpreter extends Jep {

  private static boolean initialized = false;

  public TSharedInterpreter(JepConfig config) throws JepException {
    super(config, false);
  }

  @Override
  protected void configureInterpreter(JepConfig config) throws JepException {
    synchronized (TSharedInterpreter.class) {
      super.configureInterpreter(config);
      if (!initialized) {
        setupJavaImportHook(null);
        initialized = true;
      }
    }
  }
}

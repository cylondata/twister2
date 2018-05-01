//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.common.logging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import edu.iu.dsc.tws.common.config.Config;

/**
 * A helper class to init corresponding LOGGER setting
 * loggerInit() is required to invoke before any logging
 * <p>
 * Credits: com.twitter.heron.common.utils.logging.LoggingHelper
 */
public final class LoggingHelper {
  private static final String FORMAT_PROP_KEY = "java.util.logging.SimpleFormatter.format";
  public static final String DEFAULT_FORMAT = "[%1$tF %1$tT %1$tz] [%4$s] %3$s: %5$s %6$s %n";

  private LoggingHelper() {
  }

  /**
   * Init java util logging with default format
   *
   * @param isRedirectStdOutErr whether we redirect std out&amp;err
   */
  public static void loggerInit(boolean isRedirectStdOutErr) throws IOException {
    loggerInit(isRedirectStdOutErr, DEFAULT_FORMAT);
  }

  /**
   * Init java util logging
   *
   * @param isRedirectStdOutErr whether we redirect std out&amp;err
   * @param format the format to log
   */
  public static void loggerInit(boolean isRedirectStdOutErr, String format)
      throws IOException {
    // Set the java util logging format
    setLoggingFormat(format);

    Logger rootLogger = Logger.getLogger("");

    if (isRedirectStdOutErr) {

      // Remove ConsoleHandler if present, to avoid StackOverflowError.
      // ConsoleHandler writes to System.err and since we are redirecting
      // System.err to Logger, it results in an infinite loop.
      for (Handler handler : rootLogger.getHandlers()) {
        if (handler instanceof ConsoleHandler) {
          rootLogger.removeHandler(handler);
        }
      }

      // now rebind stdout/stderr to logger
      Logger logger;
      LoggingOutputStream los;

      logger = Logger.getLogger("stdout");
      los = new LoggingOutputStream(logger, StdOutErrLevel.STDOUT);
      System.setOut(new PrintStream(los, true));

      logger = Logger.getLogger("stderr");
      los = new LoggingOutputStream(logger, StdOutErrLevel.STDERR);
      System.setErr(new PrintStream(los, true));
    }
  }

  public static void setLoggingFormat(String format) {
    System.setProperty(FORMAT_PROP_KEY, format);
  }

  public static void addLoggingHandler(Handler handler) {
    Logger.getLogger("").addHandler(handler);
  }

  public static void setLogLevel(String level) {
    setLogLevel(Level.parse(level));
  }

  public static void setLogLevel(Level level) {
    Logger rootLogger = Logger.getLogger("");
    for (Handler handler : rootLogger.getHandlers()) {
      handler.setLevel(level);
    }

    rootLogger.setLevel(level);
  }

  /**
   * Initialize a <tt>FileHandler</tt> to write to a set of files
   * with optional append.  When (approximately) the given limit has
   * been written to one file, another file will be opened.  The
   * output will cycle through a set of count files.
   * The pattern of file name should be: ${workerId}.log.index
   * <p>
   * The <tt>FileHandler</tt> is configured based on <tt>LogManager</tt>
   * properties (or their default values) except that the given pattern
   * argument is used as the filename pattern, the file limit is
   * set to the limit argument, and the file count is set to the
   * given count argument, and the append mode is set to the given
   * <tt>append</tt> argument.
   * <p>
   * The count must be at least 1.
   *
   * @param limit the maximum number of bytes to write to any one file
   * @param count the number of files to use
   * @param append specifies append mode
   * @throws IOException if there are IO problems opening the files.
   * @throws SecurityException if a security manager exists and if
   * the caller does not have <tt>LoggingPermission("control")</tt>.
   * @throws IllegalArgumentException if {@code limit < 0}, or {@code count < 1}.
   * @throws IllegalArgumentException if pattern is an empty string
   */
  public static FileHandler getFileHandler(String workerId,
                                           String loggingDir,
                                           boolean append,
                                           int limit,
                                           int count) throws IOException, SecurityException {

    String pattern = loggingDir + "/" + workerId + ".log.%g";


    FileHandler fileHandler = new FileHandler(pattern, limit, count, append);
    fileHandler.setFormatter(new SimpleFormatter());
    fileHandler.setEncoding(StandardCharsets.UTF_8.toString());

    return fileHandler;
  }

  /**
   * this method is called to setup logging parameters
   * and file handler
   * @param config
   * @param logDir
   * @param logFile
   */
  public static void setupLogging(Config config, String logDir, String logFile) {

    // get the value of twister2.logging.maximum.size.mb
    // make it MB
    int maxFileSize = LoggingContext.maxLogFileSizeBytes(config);

    // get the value of twister2.logging.maximum.files
    int maxFiles = LoggingContext.maxLogFiles(config);

    // get the value of twister2.logging.redirect.sysouterr
    boolean redirectStdOutAndStdErr = LoggingContext.redirectSysOutErr(config);

    boolean appendToFile = false;

    try {
      LoggingHelper.loggerInit(redirectStdOutAndStdErr);

      LoggingHelper.addLoggingHandler(
          LoggingHelper.getFileHandler(logFile, logDir, appendToFile, maxFileSize, maxFiles));

    } catch (IOException e) {
      System.err.println("Exception when initializing the Logger. ");
      throw new RuntimeException(e);
    }
//    LOG.info("Logging setup done.");
  }


  public static final class StdOutErrLevel extends Level {
    private static final long serialVersionUID = -3442332825945855738L;

    /**
     * Level for STDOUT activity.
     */
    public static final Level STDOUT =
        new StdOutErrLevel("STDOUT", Level.INFO.intValue() + 53);
    /**
     * Level for STDERR activity
     */
    public static final Level STDERR =
        new StdOutErrLevel("STDERR", Level.INFO.intValue() + 54);

    /**
     * Private constructor
     */
    private StdOutErrLevel(String name, int value) {
      super(name, value);
    }

    /**
     * Method to avoid creating duplicate instances when deserializing the
     * object.
     *
     * @return the singleton instance of this <code>Level</code> value in this
     * classloader
     * @throws java.io.ObjectStreamException If unable to deserialize
     */
    protected Object readResolve()
        throws ObjectStreamException {
      if (this.intValue() == STDOUT.intValue()) {
        return STDOUT;
      }
      if (this.intValue() == STDERR.intValue()) {
        return STDERR;
      }
      throw new InvalidObjectException("Unknown instance :" + this);
    }
  }

  /**
   * An OutputStream that writes contents to a Logger upon each call to flush()
   */
  public static class LoggingOutputStream extends ByteArrayOutputStream {

    private String lineSeparator;

    private Logger logger;
    private Level level;

    /**
     * Constructor
     *
     * @param logger Logger to write to
     * @param level Level at which to write the log message
     */
    public LoggingOutputStream(Logger logger, Level level) {
      super();
      this.logger = logger;
      this.level = level;
      lineSeparator = System.getProperty("line.separator");
    }

    /**
     * upon flush() write the existing contents of the OutputStream
     * to the logger as a log record.
     *
     * @throws java.io.IOException in case of error
     */
    public void flush() throws IOException {

      String record;
      synchronized (this) {
        super.flush();
        record = this.toString();
        super.reset();

        if (record.length() == 0 || record.equals(lineSeparator)) {
          // avoid empty records
          return;
        }

        logger.logp(level, "", "", record);
      }
    }
  }
}


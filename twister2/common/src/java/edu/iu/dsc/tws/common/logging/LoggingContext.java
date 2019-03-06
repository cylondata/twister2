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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;

import edu.iu.dsc.tws.common.config.Config;

public final class LoggingContext {

  private static Properties loggingProperties = new Properties();

  static {
    String loggerFile = System.getProperty("java.util.logging.config.file");
    if (loggerFile != null) {
      try {
        loggingProperties.load(new FileReader(new File(loggerFile)));
      } catch (IOException e) {
        //silently fail
      }
    }
  }

  private LoggingContext() {
  }

  public static final String LOGGING_LEVEL_DEFAULT = "INFO";

  /**
   * This will be handled by logger.properties file with standard parameters
   *
   * @deprecated use logger.properties
   */
  @Deprecated
  public static final String LOGGING_LEVEL = "twister2.logging.level";

  /**
   * This will be handled by logger.properties file with standard parameters
   *
   * @deprecated use logger.properties
   */
  @Deprecated
  public static final boolean PERSISTENT_LOGGING_REQUESTED_DEFAULT = false;

  /**
   * This will be handled by logger.properties file with standard parameters
   *
   * @deprecated use logger.properties
   */
  @Deprecated
  public static final String PERSISTENT_LOGGING_REQUESTED = "persistent.logging.requested";

  // max log file size in MB
  public static final int MAX_LOG_FILE_SIZE_DEFAULT = 100;

  /**
   * This will be handled by logger.properties file with standard parameters
   *
   * @deprecated use logger.properties
   */
  @Deprecated
  public static final String MAX_LOG_FILE_SIZE = "twister2.logging.max.file.size.mb";

  // max log files for a worker
  public static final int MAX_LOG_FILES_DEFAULT = 5;
  /**
   * This will be handled by logger.properties file with standard parameters
   *
   * @deprecated use logger.properties
   */
  @Deprecated
  public static final String MAX_LOG_FILES = "twister2.logging.maximum.files";

  // the logging format property
  public static final String DEFAULT_FORMAT =
      "[%1$tF %1$tT %1$tz] [%4$s] [%7$s] [%8$s] %3$s: %5$s %6$s %n";
  public static final String TW2_FORMAT_PROP_KEY =
      "edu.iu.dsc.tws.common.logging.Twister2LogFormatter.format";

  // whether redirect System.out and System.err to log files
  public static final boolean REDIRECT_SYS_OUT_ERR_DEFAULT = false;
  /**
   * This will be handled by logger.properties file with standard parameters
   *
   * @deprecated use logger.properties
   */
  @Deprecated
  public static final String REDIRECT_SYS_OUT_ERR = "twister2.logging.redirect.sysouterr";

  public static String loggingLevel(Config cfg) {
    //giving priority to logger.properties
    return loggingProperties.getProperty(
        ".level",
        cfg.getStringValue(LOGGING_LEVEL, LOGGING_LEVEL_DEFAULT)
    );
  }

  public static boolean persistentLoggingRequested(Config cfg) {
    String handlers = loggingProperties.getProperty("handlers", "");
    for (String className : handlers.split(",")) {
      if (className.equals(FileHandler.class.getCanonicalName())
          || className.equals(Twister2FileLogHandler.class.getCanonicalName())) {
        return true;
      }
    }
    return cfg.getBooleanValue(PERSISTENT_LOGGING_REQUESTED, PERSISTENT_LOGGING_REQUESTED_DEFAULT);
  }

  public static int maxLogFileSizeBytes(Config cfg) {
    //giving priority to logger.properties
    String size = loggingProperties.getProperty(Twister2FileLogHandler.LIMIT);
    try {
      return Integer.valueOf(size);
    } catch (Exception ex) {
      return cfg.getIntegerValue(MAX_LOG_FILE_SIZE, MAX_LOG_FILE_SIZE_DEFAULT) * 1024 * 1024;
    }
  }

  public static int maxLogFiles(Config cfg) {
    //giving priority to logger.properties
    String count = loggingProperties.getProperty(Twister2FileLogHandler.COUNT);
    try {
      return Integer.valueOf(count);
    } catch (Exception ex) {
      return cfg.getIntegerValue(MAX_LOG_FILES, MAX_LOG_FILES_DEFAULT);
    }
  }

  public static boolean appendToFile() {
    //giving priority to logger.properties
    String count = loggingProperties.getProperty(Twister2FileLogHandler.APPEND);
    try {
      return Boolean.valueOf(count);
    } catch (Exception ex) {
      return false;
    }
  }

  public static String getFileEncoding() {
    return loggingProperties.getProperty(Twister2FileLogHandler.ENCODING,
        StandardCharsets.UTF_8.toString());
  }

  public static boolean redirectSysOutErr(Config cfg) {
    //giving priority to logger.properties
    String redirect = loggingProperties.getProperty(Twister2FileLogHandler.REDIRECT_SYS);
    try {
      return Boolean.valueOf(redirect);
    } catch (Exception ex) {
      return cfg.getBooleanValue(REDIRECT_SYS_OUT_ERR, REDIRECT_SYS_OUT_ERR_DEFAULT);
    }
  }

  public static Formatter getFileLogFormatter() {
    String clazz = loggingProperties.getProperty(Twister2FileLogHandler.FORMATTER);
    if (clazz == null) {
      return new Twister2LogFormatter();
    } else {
      try {
        Class<?> clz = ClassLoader.getSystemClassLoader().loadClass(clazz);
        return (Formatter) clz.newInstance();
      } catch (Exception e) {
        return new Twister2LogFormatter();
      }
    }
  }

  public static String loggingFormat() {
    String definedFormat = loggingProperties.getProperty(TW2_FORMAT_PROP_KEY);
    if (definedFormat != null) {
      try {
        String formated = String.format(definedFormat, new Date(), "", "", "", "", "", "", "");
      } catch (IllegalArgumentException ilEx) {
        ilEx.printStackTrace();
        definedFormat = DEFAULT_FORMAT;
      }
    } else {
      definedFormat = DEFAULT_FORMAT;
    }

    return definedFormat;
  }
}

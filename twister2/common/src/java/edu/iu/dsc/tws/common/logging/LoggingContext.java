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

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.FileSystemContext;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

public final class LoggingContext {

  public static final String LOGGER_PROPERTIES_FILE = "common/logger.properties";

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

  // max log file size in MB
  public static final int MAX_LOG_FILE_SIZE_DEFAULT = 100;

  // max log files for a worker
  public static final int MAX_LOG_FILES_DEFAULT = 5;

  // the logging format property
  public static final String DEFAULT_FORMAT =
      "[%1$tF %1$tT %1$tz] [%4$s] [%7$s] [%8$s] %3$s: %5$s %6$s %n";
  public static final String TW2_FORMAT_PROP_KEY =
      "edu.iu.dsc.tws.common.logging.Twister2LogFormatter.format";

  // whether redirect System.out and System.err to log files
  public static final boolean REDIRECT_SYS_OUT_ERR_DEFAULT = false;

  public static final String LOGGING_STORAGE_TYPE = "twister2.logging.storage.type";

  public static String loggingLevel() {
    //giving priority to logger.properties
    return loggingProperties.getProperty(".level", LOGGING_LEVEL_DEFAULT);
  }

  public static boolean fileLoggingRequested() {
    String handlers = loggingProperties.getProperty("handlers", "");
    for (String className : handlers.split(",")) {
      if (className.equals(FileHandler.class.getCanonicalName())
          || className.equals(Twister2FileLogHandler.class.getCanonicalName())) {
        return true;
      }
    }
    return false;
  }

  public static int maxLogFileSizeBytes() {
    //giving priority to logger.properties
    String size = loggingProperties.getProperty(Twister2FileLogHandler.LIMIT);
    try {
      return Integer.valueOf(size);
    } catch (Exception ex) {
      return MAX_LOG_FILE_SIZE_DEFAULT * 1024 * 1024;
    }
  }

  public static int maxLogFiles() {
    //giving priority to logger.properties
    String count = loggingProperties.getProperty(Twister2FileLogHandler.COUNT);
    try {
      return Integer.valueOf(count);
    } catch (Exception ex) {
      return MAX_LOG_FILES_DEFAULT;
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

  public static boolean redirectSysOutErr() {
    //giving priority to logger.properties
    String redirect = loggingProperties.getProperty(Twister2FileLogHandler.REDIRECT_SYS);
    try {
      return Boolean.valueOf(redirect);
    } catch (Exception ex) {
      return REDIRECT_SYS_OUT_ERR_DEFAULT;
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
        definedFormat = DEFAULT_FORMAT;
      }
    } else {
      definedFormat = DEFAULT_FORMAT;
    }

    return definedFormat;
  }

  public static String loggingStorageType(Config config) {
    return config.getStringValue(LOGGING_STORAGE_TYPE, "volatile");
  }

  public static String loggingDir(Config config) {
    String storageType = loggingStorageType(config);
    String rootDir;
    switch (storageType) {
      case "volatile":
        rootDir = FileSystemContext.volatileStorageRoot(config);
        break;
      case "persistent":
        if (FileSystemContext.persistentStorageType(config).equals("hdfs")) {
          throw new Twister2RuntimeException("hdfs is not supported for logging. "
              + "Please either specify logging.storage.type as volatile or "
              + "persistent.storage.type as nfs or local.");
        }
        rootDir = FileSystemContext.persistentStorageRoot(config);
        break;

      default:
        throw new Twister2RuntimeException("unsupported logging storage type: " + storageType);
    }

    return rootDir + File.separator + "logs";
  }
}

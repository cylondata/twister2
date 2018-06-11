//  Copyright 2017 Twitter. All rights reserved.
//
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
package edu.iu.dsc.tws.common.config;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TokenSub {
  private static final Logger LOG = Logger.getLogger(TokenSub.class.getName());

  // Pattern to match an URL - just looks for double forward slashes //
  private static final Pattern URL_PATTERN = Pattern.compile("(.+)://(.+)");
  private static final Pattern TOKEN_PATTERN = Pattern.compile("^\\$\\{([A-Z_0-9]+)}$");

  private TokenSub() {
  }

  /**
   * Given a static config map, substitute occurrences of ${TWISTER2_*} variables
   * in the provided path string
   *
   * @param config a static map config object of key value pairs
   * @param pathString string representing a path including ${TWISTER2_*} variables
   * @return String string that represents the modified path
   */
  public static String substitute(Config config, String pathString,
                                  Map<String, ConfigEntry> substitutes) {

    // trim the leading and trailing spaces
    String trimmedPath = pathString.trim();

    if (isURL(trimmedPath)) {
      return substituteURL(config, trimmedPath, substitutes);
    }

    // get platform independent file separator
    String fileSeparator = Matcher.quoteReplacement(File.separator);

    // split the trimmed path into a list of components
    List<String> fixedList = Arrays.asList(trimmedPath.split(fileSeparator));
    List<String> list = new LinkedList<>(fixedList);

    // substitute various variables
    for (int i = 0; i < list.size(); i++) {
      String elem = list.get(i);

      if ("${HOME}".equals(elem) || "~".equals(elem)) {
        list.set(i, System.getProperty("user.home"));

      } else if ("${JAVA_HOME}".equals(elem)) {
        String javaPath = System.getenv("JAVA_HOME");
        if (javaPath != null) {
          list.set(i, javaPath);
        }
      } else if ("${HADOOP_HOME}".equals(elem)) {
        String hadoopPath = System.getenv("HADOOP_HOME");
        if (hadoopPath != null) {
          list.set(i, hadoopPath);
        }
      } else if (isToken(elem)) {
        Matcher m = TOKEN_PATTERN.matcher(elem);
        if (m.matches()) {
          String token = m.group(1);
          try {
            ConfigEntry entry = substitutes.get(token);
            if (entry == null) {
              LOG.warning("We cannot find the substitution entry for token: " + token);
              continue;
            }
            String value = config.getStringValue(entry.getKey());
            if (value == null) {
              throw new IllegalArgumentException(String.format("Config value %s contains "
                      + "substitution token %s but the corresponding config setting %s not found",
                  pathString, elem, entry.getKey()));
            }
            list.set(i, value);
          } catch (IllegalArgumentException e) {
            LOG.warning(String.format("Config value %s contains substitution token %s which is "
                    + "not defined in the Key enum, which is required for token substitution",
                pathString, elem));
          }
        }
      }
    }

    return combinePaths(list);
  }

  /**
   * Given a string, check if it is a URL - URL, according to our definition is
   * the presence of two consecutive forward slashes //
   *
   * @param pathString string representing a path
   * @return true if the pathString is a URL, else false
   */
  private static boolean isURL(String pathString) {
    return URL_PATTERN.matcher(pathString).matches();
  }

  /**
   * Given a static config map, substitute occurrences of ${TWISTER2_*} variables
   * in the provided URL
   *
   * @param config a static map config object of key value pairs
   * @param pathString string representing a path including ${TWISTER2_*} variables
   * @return String string that represents the modified path
   */
  private static String substituteURL(Config config, String pathString,
                                      Map<String, ConfigEntry> substitutes) {
    Matcher m = URL_PATTERN.matcher(pathString);
    if (m.matches()) {
      return String.format("%s://%s", m.group(1), substitute(config, m.group(2), substitutes));
    }
    return pathString;
  }

  private static boolean isToken(String pathString) {
    Matcher m = TOKEN_PATTERN.matcher(pathString);
    return m.matches() && m.groupCount() > 0;
  }

  /**
   * Given a list of strings, concatenate them to form a file system
   * path
   *
   * @param paths a list of strings to be included in the path
   * @return String string that gives the file system path
   */
  private static String combinePaths(List<String> paths) {
    File file = new File(paths.get(0));

    for (int i = 1; i < paths.size(); i++) {
      file = new File(file, paths.get(i));
    }

    return file.getPath();
  }
}


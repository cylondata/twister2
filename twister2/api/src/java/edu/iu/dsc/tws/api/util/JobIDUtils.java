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
package edu.iu.dsc.tws.api.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

public final class JobIDUtils {

  // JobID format: <username>-<jobName>-<timestamp>
  // max 49 characters= <9 chars>-<30 chars>-<8 chars>

  // we limit jobID to 49, because we use jobID when creating resources in K8s clusters
  // We use jobID as part of statefulset, service, and label names
  // the names of these resources can be at most 63 characters in K8s
  // We add some prefix or suffixes to jobID when creating these names
  //
  // statefulset pods has the following label
  // controller-revision-hash=<pod-name>-<hash>
  // hash part can be 10 characters
  // so, the pod name can be at most 52 chars, since jm pod name has a 3 char suffix "-jm"
  // jobID can have at most 49 chars
  private static final int MAX_JOB_ID_LENGTH = 49;

  private static final int MAX_USER_NAME_LENGTH = 9;
  private static final int MAX_TIMESTAMP_LENGTH = 8;
  private static final int MAX_JOB_NAME_LENGTH_IN_JOB_ID = 30;

  private JobIDUtils() { }

  public static String generateJobID(String jobName, String userName) {

    String jobNameInID = jobName;
    if (!nameConformsToK8sNamingRules(jobName, MAX_JOB_NAME_LENGTH_IN_JOB_ID)) {
      jobNameInID = convertToK8sFormat(jobName, MAX_JOB_NAME_LENGTH_IN_JOB_ID);
    }

    String userNameInID = userName;
    if (!nameConformsToK8sNamingRules(userName, MAX_USER_NAME_LENGTH)) {
      userNameInID = convertToK8sFormat(userName, MAX_USER_NAME_LENGTH);
    }

    String timestamp = timestamp();
    // leave out characters from the beginning if it is longer than max
    // this should not happen until the year 2105
    if (timestamp.length() > MAX_TIMESTAMP_LENGTH) {
      timestamp = timestamp.substring(timestamp.length() - MAX_TIMESTAMP_LENGTH);
    }

    if (userNameInID == null || "".equals(userNameInID)) {
      return String.format("%s-%s", jobNameInID, timestamp);
    } else {
      return String.format("%s-%s-%s", userNameInID, jobNameInID, timestamp);
    }
  }


  /**
   * whether given jobName conforms to k8s naming rules and max length
   * @param jName
   * @return
   */
  public static boolean nameConformsToK8sNamingRules(String jName, int maxLength) {

    // first we need to check the length of the name
    if (jName.length() > maxLength) {
      return false;
    }

    // first character is a lowercase letter from a to z
    // last character is an alphanumeric character: [a-z0-9]
    // in between alphanumeric characters and dashes
    // first character is mandatory. It has to be at least 1 char in length
    if (jName.matches("[a-z]([-a-z0-9]*[a-z0-9])?")) {
      return true;
    }

    return false;
  }

  /**
   * we perform the following actions:
   *   shorten the length of the job name if needed
   *   replace underscore and dot characters with dashes
   *   convert all letters to lower case characters
   *   delete non-alphanumeric characters excluding dots and dashes
   *   replace the first char with "a", if it is not a letter in between a-z
   *   replace the last char with "z", if it is dash
   * @param name
   * @return
   */
  public static String convertToK8sFormat(String name, int maxLength) {

    // replace underscores with dashes if any
    String modifiedName = name.replace("_", "-");
    // replace dots with dashes if any
    modifiedName = modifiedName.replace(".", "-");

    // convert to lower case
    modifiedName = modifiedName.toLowerCase(Locale.ENGLISH);

    // delete all non-alphanumeric characters excluding dashes
    modifiedName = modifiedName.replaceAll("[^a-z0-9\\-]", "");

    // make sure the first char is a letter
    // if not, replace it  with "a"
    if (modifiedName.matches("[^a-z][-a-z0-9]*")) {
      modifiedName = "a" + modifiedName.substring(1);
    }

    // make sure the last char is not dash
    // if it is, replace it  with "z"
    if (modifiedName.matches("[-a-z0-9]*[-]")) {
      modifiedName = modifiedName.substring(0, modifiedName.length() - 1) + "z";
    }

    // shorten the job name if needed
    if (modifiedName.length() > maxLength) {
      modifiedName = modifiedName.substring(0, maxLength);
    }

    return modifiedName;
  }

  /**
   * create an alpha numeric string from time stamp value
   * current time - 01/01/2019
   * @return
   */
  public static String timestamp() {
    DateFormat sdf = new SimpleDateFormat("MM/dd/yyyy", Locale.ENGLISH);
    Date startDate = null;
    try {
      startDate = sdf.parse("01/01/2019");
    } catch (ParseException e) {
      throw new Twister2RuntimeException("Totally unexpected error when creating Date object");
    }
    long diff = System.currentTimeMillis() - startDate.getTime();
    return base36(diff);
  }

  /**
   * convert given time stamp to base36 alpha numeric string
   * @param ts
   * @return
   */
  public static String base36(long ts) {
    String digits = "0123456789abcdefghijklmnopqrstuvwxyz";
    int base = digits.length();
    long n = ts;
    String s = "";

    while (n > 0) {
      long d = n % base;
      s = digits.charAt((int) d) + s;
      n = n / base;
    }
    return s;
  }

}

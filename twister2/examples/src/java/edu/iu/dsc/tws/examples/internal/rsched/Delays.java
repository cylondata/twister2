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
package edu.iu.dsc.tws.examples.internal.rsched;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class Delays {

  private Delays() { }

  public static final String LOGS_DIR = System.getProperty("user.home") + "/t2-logs/";

  public static void main(String[] args) {

    String jobID;

    if (args.length == 1) {
      jobID = args[0];
    } else {
      jobID = readJobID();
    }

    String jobDir = LOGS_DIR + jobID;
    int workers = new File(jobDir).listFiles().length - 2;
    long jst = jobSubmitTime(jobDir);
    long ld = launchDelay(jobDir);

    System.out.println(jobID + "\t" + workers + "\tld\t" + ld);

    for (int i = 0; i < workers; i++) {
      String wlog = jobDir + "/worker-" + i + ".log";
      String delays = workerDelays(jst, wlog);
      System.out.println(i + "\t" + delays);
    }

  }

  public static String workerDelays(long jst, String wFile) {
    List<String> lines = readFileLines(wFile);
    String delays = "";

    for (String line: lines) {
      if (line.contains("timestamp")) {
        String trimmedLine = line.trim();
        String ts = trimmedLine.substring(trimmedLine.lastIndexOf(" ") + 1);
        long delay = Long.parseLong(ts) - jst;
        delays += delay + "\t";
      }
    }

    return delays.trim();
  }

  public static String readJobID() {
    String jobIDFile = System.getProperty("user.home") + "/.twister2/last-job-id.txt";
    return readFileLines(jobIDFile).get(0);
  }

  public static long jobSubmitTime(String jobDir) {
    String jstFile = jobDir + "/jobSubmitTime.txt";
    return Long.parseLong(readFileLines(jstFile).get(0));
  }

  public static long launchDelay(String jobDir) {
    String ldFile = jobDir + "/launch-delay.txt";
    return Long.parseLong(readFileLines(ldFile).get(0));
  }

  public static List<String> readFileLines(String filename) {
    Path path = new File(filename).toPath();
    try {
      List<String> lines = Files.readAllLines(path);
      return lines;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}

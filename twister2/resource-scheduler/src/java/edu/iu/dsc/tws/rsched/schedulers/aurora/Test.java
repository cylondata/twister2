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
package edu.iu.dsc.tws.rsched.schedulers.aurora;

import java.io.File;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;

import edu.iu.dsc.tws.rsched.utils.ProcessUtils;

public final class Test {
  public static void main(String[] args) {
    System.out.println("Helloooo test from twister2");
//    testProcessUtils();
    testLoadConfig();
//    testJobSubmission(args);
  }

  private Test() {

  }

  public static void testProcessUtils() {
    System.out.println("intestProcessUtils");
    String[] command = {"ls"};
    File workingDirectory = new File(".");
    String logFile = "log.txt";
    Process p = ProcessUtils.runASyncProcess(command, workingDirectory, logFile);
    try {
      p.waitFor();
      System.out.println("process terminated. ");
    } catch (InterruptedException e) {
      System.out.println("process threw an exception before termination.");
      e.printStackTrace();
    }
  }

  public static void testJobSubmission(String[] args) {
    if (args == null) {
      System.out.println("provide either create or kill");
      return;
    }

    if (args[0].equals("create")) {
      testJobCreate();
    } else if (args[0].equals("kill")) {
      testJobKill();

    } else {
      System.out.println(args[0] + " not supported action.");

    }
  }

  /**
   * testJobCreate
   */
  public static void testJobCreate() {

    String auroraFile = "/root/twister2/twister2test.aurora";
    String cluster = "example";
    String role = "www-data";
    String env = "devel";
    String jobName = "hello";

    AuroraClientController controller = new AuroraClientController(
        cluster, role, env, jobName, true);

    Config.Builder builder = Config.newBuilder();
    builder.put(AuroraClientContext.AURORA_CLUSTER_NAME, cluster);
    builder.put(AuroraClientContext.ROLE, role);
    builder.put(AuroraClientContext.ENVIRONMENT, env);
    builder.put(AuroraClientContext.AURORA_JOB_NAME, jobName);

    builder.put(AuroraClientContext.AURORA_SCRIPT.getKey(), auroraFile);
//    builder.put(AuroraClientContext.TWISTER2_PACKAGE_PATH, "/root/twister2");
//    builder.put(AuroraClientContext.TWISTER2_PACKAGE_FILE, "twister2-client.tar.gz");

    builder.put(AuroraClientContext.NUMBER_OF_CONTAINERS, "1");
    builder.put(AuroraClientContext.CPUS_PER_CONTAINER, "1");
    String ramAndDiskSize = "" + 1 * 1024 * 1024 * 1024; // 1GB in bytes
    builder.put(AuroraClientContext.RAM_PER_CONTAINER, ramAndDiskSize);
    builder.put(AuroraClientContext.DISK_PER_CONTAINER, ramAndDiskSize);
    Config config = builder.build();

    System.out.println("number of config parameters: " + config.size());
    System.out.println(config);

    // get environment variables from config
    Map<AuroraField, String> bindings = AuroraLauncher.constructEnvVariables(config);
    // print all environment variables for debugging
    AuroraLauncher.printEnvs(bindings);

    boolean result = controller.createJob(bindings, auroraFile);
    if (result) {
      System.out.println("job submission is successfull");
    } else {
      System.out.println("job submission is unsuccessfull");
    }
  }

  public static void testJobKill() {
    String cluster = "example";
    String role = "www-data";
    String env = "devel";
    String jobName = "hello";

    AuroraClientController controller = new AuroraClientController(
        cluster, role, env, jobName, true);
    boolean result = controller.killJob();
    if (result) {
      System.out.println("job kill is successfull");
    } else {
      System.out.println("job kill is unsuccessfull");
    }
  }

  public static void testLoadConfig() {
    String twister2Home = "/home/auyar/projects/temp";
    String configPath = "/home/auyar/projects/temp/twister2-dist/conf/aurora";
    Config config = ConfigLoader.loadConfig(twister2Home, configPath);
    int size = config.size();
    System.out.println("number of configs: " + size);
    Set<String> keys = config.getKeySet();

    for (String key : keys) {
      System.out.println(key + ": " + config.get(key).toString());
    }
  }

}

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

import edu.iu.dsc.tws.rsched.utils.ProcessUtils;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.config.Config;

public class Test {
  public static void main(String [] args){
    System.out.println("Helloooo test from twister2");
//    testProcessUtils();
//    testLoadConfig();
    testJobSubmission(args);
  }

  public static void testProcessUtils(){
    System.out.println("intestProcessUtils");
    String command[] ={"ls"};
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

  public static void testJobSubmission(String [] args){
    if(args == null) {
      System.out.println("provide either create or kill");
      return;
    }

    if(args[0].equals("create"))
      testJobCreate();
    else if(args[0].equals("kill"))
      testJobKill();
    else
      System.out.println(args[0] + " not supported action.");
  }

  public static void testJobCreate(){
    String auroraFile = "/root/twister2/twister2test.aurora";
    String cluster = "example";
    String role = "www-data";
    String env = "devel";
    String jobName = "hello";
    AuroraClientController controller = new AuroraClientController(jobName, cluster, role, env, auroraFile, true);

    HashMap<AuroraField, String> bindings = new HashMap<AuroraField, String>();
    bindings.put(AuroraField.CLUSTER, "example");
    bindings.put(AuroraField.ENVIRONMENT, "devel");
    bindings.put(AuroraField.ROLE, "www-data");
    bindings.put(AuroraField.JOB_NAME, "hello");
    bindings.put(AuroraField.NUMBER_OF_CONTAINERS, "1");
    bindings.put(AuroraField.CPUS_PER_CONTAINER, "1");
    String ramAndDiskSize = "" + 1*1024*1024*1024; // 1GB in bytes
    bindings.put(AuroraField.RAM_PER_CONTAINER, ramAndDiskSize);
    bindings.put(AuroraField.DISK_PER_CONTAINER, ramAndDiskSize);
//    bindings.put(AuroraField.DISK_PER_CONTAINER, "1*GB"); // it requires int

    boolean result = controller.createJob(bindings);
    if(result)
      System.out.println("job submission is successfull");
    else
      System.out.println("job submission is unsuccessfull");
  }

  public static void testJobCreate2(){
    String auroraFile = "/vagrant/aurora_hello/pyhello.aurora";
    String jobName = "hello";
    String cluster = "example";
    String role = "www-data";
    String env = "devel";
    AuroraClientController controller = new AuroraClientController(jobName, cluster, role, env, auroraFile, true);

    HashMap<AuroraField, String> bindings = new HashMap<AuroraField, String>();
    boolean result = controller.createJob(bindings);
    if(result)
      System.out.println("job submission is successfull");
    else
      System.out.println("job submission is unsuccessfull");
  }

  public static void testJobKill(){
    String auroraFile = "/vagrant/aurora_hello/pyhello.aurora";
    String jobName = "hello";
    String cluster = "example";
    String role = "www-data";
    String env = "devel";
    AuroraClientController controller = new AuroraClientController(jobName, cluster, role, env, auroraFile, true);
    boolean result = controller.killJob();
    if(result)
      System.out.println("job submission is successfull");
    else
      System.out.println("job submission is unsuccessfull");
  }

  public static void testLoadConfig(){
    String twister2Home = "/home/auyar/projects/twister2";
    String configPath = "/home/auyar/projects/twister2/twister2/config/src/yaml/conf/aurora";
    Config config = ConfigLoader.loadConfig(twister2Home, configPath);
    int size = config.size();
    System.out.println("number of configs: "+size);
    Set<String> keys = config.getKeySet();

    for (String key: keys) {
      try {
        System.out.println(key + ": " + config.getStringValue(key));
      }catch(java.lang.ClassCastException e){
        System.out.println(key+" ClassCastException: "+e.getMessage());
      }
    }
  }

  public static void printConfig(Config config){
    int size = config.size();
    System.out.println("number of configs: "+size);
    Set<String> keys = config.getKeySet();

    for (String key: keys) {
      try {
        System.out.println(key + ": " + config.getStringValue(key));
      }catch(java.lang.ClassCastException e){
        System.out.println(key+" ClassCastException: "+e.getMessage());
      }
    }
  }

  public static void printEnvs(Map<AuroraField, String> envs){
    Set<AuroraField> keys = envs.keySet();

    for (AuroraField key: keys) {
      System.out.println(key + ": " + envs.get(key));
    }

  }

}

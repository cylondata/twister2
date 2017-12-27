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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

/**
 * This is the class to submit Twister2 jobs to AuroraCluster
 */
public class AuroraProcess {
  private static final Logger LOG = Logger.getLogger(AuroraProcess.class.getName());
  private AuroraProcess() {
  }

  public static void main(String[] args) {
    Options cmdOptions = null;
    try {

      // get command line parameters
      cmdOptions = setupOptions();
      CommandLineParser parser = new DefaultParser();
      CommandLine cmd = parser.parse(cmdOptions, args);

      // load the configurations from config files
      // we are loading the configuration for all the components
      Config config = loadConfigurations(cmd);
      System.out.println("all config entries");
      Test.printConfig(config);

      // get environment variables to set when executing aurora file
      Map<AuroraField, String> envs = getEnvVariables(config);
//      System.out.println("\nall aurora environment variables");
//      Test.printEnvs(envs);

      //construct the controller to submit the job to Aurora Scheduler
      AuroraClientController controller = new AuroraClientController(SchedulerContext.jobName(config),
          AuroraClientContext.cluster(config),
          AuroraClientContext.role(config),
          AuroraClientContext.environment(config),
          Context.auroraScript(config),
          true);

      boolean jobSubmitted = controller.createJob(envs);
      if(jobSubmitted)
        LOG.log(Level.INFO, "job submission is successfull ...");
      else
        LOG.log(Level.SEVERE, "job submission to Aurora failed ...");

    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("AuroraProcess", cmdOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    }
  }

  /**
   * Setup the command line options for AuroraProcess
   * It gets three command line parameters:
   *   twister2_home: home directory for twister2
   *   config_dir: config directory for twister2 project
   *   cluster_name: it should be "aurora"
   * @return cli options
   */
  private static Options setupOptions() {
    Options options = new Options();

    Option twister2Home = Option.builder("t")
        .desc("The class name of the container to launch")
        .longOpt("twister2_home")
        .hasArgs()
        .argName("twister2 home")
        .required()
        .build();

    Option configDirectory = Option.builder("d")
        .desc("The config directory")
        .longOpt("config_dir")
        .hasArgs()
        .argName("configuration directory")
        .required()
        .build();

    Option clusterName = Option.builder("n")
        .desc("The clustr name")
        .longOpt("cluster_name")
        .hasArgs()
        .argName("cluster name")
        .required()
        .build();

    options.addOption(twister2Home);
    options.addOption(configDirectory);
    options.addOption(clusterName);

    return options;
  }

  /**
   * read config parameters from configuration files
   * all config files are in a single directory
   * @param cmd
   * @return Config object that has values from config files and from command line
   */
  private static Config loadConfigurations(CommandLine cmd) {
    String twister2Home = cmd.getOptionValue("twister2_home");
    String configDir = cmd.getOptionValue("config_dir");
    String clusterName = cmd.getOptionValue("cluster_name");

    LOG.log(Level.INFO, String.format("Initializing process with "
            + "twister_home: %s config_dir: %s cluster_name: %s",
        twister2Home, configDir, clusterName));

    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterName);

    return Config.newBuilder().putAll(config).
        put(AuroraClientContext.TWISTER2_HOME.getKey(), twister2Home).
        put(AuroraClientContext.TWISTER2_CLUSTER_NAME, clusterName).build();
  }

  /**
   * put relevant config parameters to a HashMap to be used as environment variables
   * @param config
   * @return
   */
  public static Map<AuroraField, String> getEnvVariables(Config config){
    HashMap<AuroraField, String> envs = new HashMap<AuroraField, String>();
    envs.put(AuroraField.CLUSTER, AuroraClientContext.cluster(config));
    envs.put(AuroraField.ENVIRONMENT, AuroraClientContext.environment(config));
    envs.put(AuroraField.ROLE, AuroraClientContext.role(config));
    envs.put(AuroraField.JOB_NAME, SchedulerContext.jobName(config));
    envs.put(AuroraField.CPUS_PER_CONTAINER, AuroraClientContext.cpusPerContainer(config));
    envs.put(AuroraField.RAM_PER_CONTAINER, AuroraClientContext.ramPerContainer(config)+"");
    envs.put(AuroraField.DISK_PER_CONTAINER, AuroraClientContext.diskPerContainer(config)+"");
    envs.put(AuroraField.NUMBER_OF_CONTAINERS, AuroraClientContext.numberOfContainers(config));
    return envs;
  }
}

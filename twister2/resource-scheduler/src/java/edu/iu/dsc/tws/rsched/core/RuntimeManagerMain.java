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
package edu.iu.dsc.tws.rsched.core;

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

public final class RuntimeManagerMain {
  private static final Logger LOG = Logger.getLogger(RuntimeManagerMain.class.getName());

  public static void main(String[] args) {
    setupOptions();

    Options cmdOptions = null;
    try {
      cmdOptions = setupOptions();
      CommandLineParser parser = new DefaultParser();
      // parse the help options first.
      CommandLine cmd = parser.parse(cmdOptions, args);

      // load the configuration
      // we are loading the configuration for all the components
      Config config = loadConfigurations(cmd);
      // normal worker
      LOG.log(Level.INFO, "The runtime controller...");
      String command = cmd.getOptionValue("command");
      executeCommand(config, command);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SubmitterMain", cmdOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    } catch (Throwable t) {
      String msg = "Un-expected error";
      LOG.log(Level.SEVERE, msg, t);
      throw new RuntimeException(msg, t);
    }
  }

  private static void executeCommand(Config cfg, String command) {
    switch (command) {
      case "kill":
        // now load the correct class to terminate the job
        ResourceAllocator resourceAllocator = new ResourceAllocator();
        resourceAllocator.terminateJob(SchedulerContext.jobName(cfg), cfg);
    }
  }

  private static Config loadConfigurations(CommandLine cmd) {
    String twister2Home = cmd.getOptionValue("twister2_home");
    String configDir = cmd.getOptionValue("config_path");
    String cluster = cmd.getOptionValue("cluster");
    String jobName = cmd.getOptionValue("job_name");
    String command = cmd.getOptionValue("command");

    LOG.log(Level.INFO, String.format("Initializing process with "
            + "twister_home: %s command: %s config_dir: %s cluster_type: %s",
        twister2Home, command, configDir, cluster));

    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + cluster);

    return Config.newBuilder().putAll(config).
        put(SchedulerContext.TWISTER2_HOME.getKey(), twister2Home).
        put(SchedulerContext.CONFIG_DIR, config).
        put(SchedulerContext.JOB_NAME, jobName).
        put(SchedulerContext.TWISTER2_CLUSTER_TYPE, cluster).build();
  }

  /**
   * Setup the command line options for the MPI process
   * @return cli options
   */
  private static Options setupOptions() {
    Options options = new Options();

    Option cluster = Option.builder("c")
        .desc("The name of the cluster configuration")
        .longOpt("cluster")
        .hasArgs()
        .argName("Cluster name")
        .required()
        .build();

    Option configDirectory = Option.builder("d")
        .desc("The config directory")
        .longOpt("config_path")
        .hasArgs()
        .argName("configuration directory")
        .required()
        .build();

    Option twister2Home = Option.builder("t")
        .desc("The class name of the container to launch")
        .longOpt("twister2_home")
        .hasArgs()
        .argName("twister2 home")
        .required()
        .build();

    Option command = Option.builder("m")
        .desc("Command name")
        .longOpt("command")
        .hasArgs()
        .argName("command")
        .required()
        .build();

    Option jobName = Option.builder("j")
        .desc("Job name")
        .longOpt("job_name")
        .hasArgs()
        .argName("job name")
        .required()
        .build();
    options.addOption(twister2Home);
    options.addOption(cluster);
    options.addOption(configDirectory);
    options.addOption(command);
    options.addOption(jobName);

    return options;
  }
}

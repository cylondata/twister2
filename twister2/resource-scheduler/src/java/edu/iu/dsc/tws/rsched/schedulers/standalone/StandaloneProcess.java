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
package edu.iu.dsc.tws.rsched.schedulers.standalone;

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
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.mpi.MPIContext;
import edu.iu.dsc.tws.rsched.schedulers.mpi.MPIProcess;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public final class StandaloneProcess {
  private static final Logger LOG = Logger.getLogger(MPIProcess.class.getName());

  private StandaloneProcess() {
  }

  public static void main(String[] args) {
    Options cmdOptions = null;
    try {
      cmdOptions = setupOptions();
      CommandLineParser parser = new DefaultParser();
      // parse the help options first.
      CommandLine cmd = parser.parse(cmdOptions, args);

      // lets determine the process id
      int rank = 0;

      // load the configuration
      // we are loading the configuration for all the components
      Config config = loadConfigurations(cmd, rank);
      // normal worker
      LOG.log(Level.FINE, "A worker process is starting...");
      worker(config, rank);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SubmitterMain", cmdOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    }
  }

  /**
   * Setup the command line options for the MPI process
   * @return cli options
   */
  private static Options setupOptions() {
    Options options = new Options();

    Option containerClass = Option.builder("c")
        .desc("The class name of the container to launch")
        .longOpt("container_class")
        .hasArgs()
        .argName("container class")
        .required()
        .build();

    Option configDirectory = Option.builder("d")
        .desc("The class name of the container to launch")
        .longOpt("config_dir")
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

    Option clusterType = Option.builder("n")
        .desc("The clustr type")
        .longOpt("cluster_type")
        .hasArgs()
        .argName("cluster type")
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
    options.addOption(containerClass);
    options.addOption(configDirectory);
    options.addOption(clusterType);
    options.addOption(jobName);

    return options;
  }

  private static Config loadConfigurations(CommandLine cmd, int id) {
    String twister2Home = cmd.getOptionValue("twister2_home");
    String container = cmd.getOptionValue("container_class");
    String configDir = cmd.getOptionValue("config_dir");
    String clusterType = cmd.getOptionValue("cluster_type");
    String jobName = cmd.getOptionValue("job_name");

    LOG.log(Level.FINE, String.format("Initializing process with "
            + "twister_home: %s container_class: %s config_dir: %s cluster_type: %s",
        twister2Home, container, configDir, clusterType));

    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);

    Config workerConfig = Config.newBuilder().putAll(config).
        put(MPIContext.TWISTER2_HOME.getKey(), twister2Home).
        put(MPIContext.CONTAINER_CLASS, container).
        put(MPIContext.TWISTER2_CONTAINER_ID, id).
        put(MPIContext.TWISTER2_CLUSTER_TYPE, clusterType).build();

    String jobDescFile = JobUtils.getJobDescriptionFilePath(jobName, workerConfig);
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);

    Config updatedConfig = JobUtils.overrideConfigs(job, config);

    updatedConfig = Config.newBuilder().putAll(updatedConfig).
        put(MPIContext.TWISTER2_HOME.getKey(), twister2Home).
        put(MPIContext.CONTAINER_CLASS, container).
        put(MPIContext.TWISTER2_CONTAINER_ID, id).
        put(MPIContext.TWISTER2_CLUSTER_TYPE, clusterType).build();
    return updatedConfig;
  }

  private static void master(Config config, int rank) {
    // lets do a barrier here so everyone is synchronized at the start
    // lets create the resource plan
    createResourcePlan(config);
  }

  private static void worker(Config config, int rank) {
    // lets create the resource plan
    ResourcePlan resourcePlan = createResourcePlan(config);

    String containerClass = MPIContext.containerClass(config);
    IContainer container;
    try {
      Object object = ReflectionUtils.newInstance(containerClass);
      container = (IContainer) object;
      LOG.log(Level.FINE, "loaded container class: " + containerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.log(Level.SEVERE, String.format("failed to load the container class %s",
          containerClass), e);
      throw new RuntimeException(e);
    }

    // now initialize the container
    container.init(config, rank, resourcePlan);
  }

  private static ResourcePlan createResourcePlan(Config config) {

    return null;
  }
}

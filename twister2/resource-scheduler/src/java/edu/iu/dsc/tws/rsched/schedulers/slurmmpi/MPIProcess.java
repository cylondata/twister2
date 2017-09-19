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
package edu.iu.dsc.tws.rsched.schedulers.slurmmpi;

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
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

import mpi.MPI;
import mpi.MPIException;


/**
 * This is the base process started by the resource scheduler. This process will
 * start the rest of the resource as needed.
 */
public final class MPIProcess {
  private static final Logger LOG = Logger.getLogger(MPIProcess.class.getName());

  private MPIProcess() {
  }

  public static void main(String[] args) {
    try {
      MPI.Init(args);

      int rank = MPI.COMM_WORLD.getRank();
      int size = MPI.COMM_WORLD.getSize();

      Options cmdOptions = setupOptions();
      CommandLineParser parser = new DefaultParser();
      // parse the help options first.
      CommandLine cmd = parser.parse(cmdOptions, args, true);

      try {
        // Now parse the required options
        cmd = parser.parse(cmdOptions, args);
      } catch (ParseException e) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SubmitterMain", cmdOptions);
        throw new RuntimeException("Error parsing command line options: ", e);
      }

      // load the configuration
      // we are loading the configuration for all the components
      Config config = loadConfigurations(cmd, rank);

      // this is the job manager`
      if (rank == 0) {
        LOG.log(Level.INFO, "This is the master process, we are not doing anything");
        // first lets do a barrier
        MPI.COMM_WORLD.barrier();
        // now wait until other processes finish
        while (true) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException ignore) {
          }
        }
      } else {
        // normal worker
        LOG.log(Level.INFO, "A worker process is starting...");
        worker(config, rank);
      }
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Failed the MPI process", e);
      throw new RuntimeException(e);
    } catch (ParseException e) {
      LOG.log(Level.SEVERE, "Invalid arguments");
      throw new RuntimeException(e);
    } finally {
      try {
        MPI.Finalize();
      } catch (MPIException e) {
      }
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

    Option clusterName = Option.builder("n")
        .desc("The clustr name")
        .longOpt("cluster_name")
        .hasArgs()
        .argName("cluster name")
        .required()
        .build();

    options.addOption(twister2Home);
    options.addOption(containerClass);
    options.addOption(configDirectory);
    options.addOption(clusterName);

    return options;
  }

  private static Config loadConfigurations(CommandLine cmd, int id) {
    String twister2Home = cmd.getOptionValue("twister2_home");
    String container = cmd.getOptionValue("c_class");
    String configDir = cmd.getOptionValue("config_dir");
    String overrideFile = cmd.getOptionValue("override_file");
    String clusterName = cmd.getOptionValue("cluster_name");

    Config config = ConfigLoader.loadConfig(twister2Home, configDir, overrideFile);
    return Config.newBuilder().putAll(config).
        put(SlurmMPIContext.TWISTER2_JOB_BASIC_CONTAINER_CLASS, container).
        put(SlurmMPIContext.TWISTER2_CONTAINER_ID, id).
        put(SlurmMPIContext.TWISTER2_CLUSTER_NAME, clusterName).build();
  }

  private static void master(Config config, int rank) {

  }

  private static void worker(Config config, int rank) {
    String containerClass = SlurmMPIContext.jobBasicContainerClass(config);
    IContainer container = null;
    try {
      Object object = ReflectionUtils.newInstance(containerClass);
      container = (IContainer) object;
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.log(Level.SEVERE, "failed to load the container class", e);
      throw new RuntimeException(e);
    }

    // lets create the resource plan
    ResourcePlan resourcePlan = new ResourcePlan(SlurmMPIContext.clusterName(config));

    // lets do a barrier here so everyone is synchronized at the start
    try {
      MPI.COMM_WORLD.barrier();
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Failed to synchronize the workers at the start");
      throw new RuntimeException(e);
    }

    // now initialize the container
    container.init(config, rank, resourcePlan);
  }
}

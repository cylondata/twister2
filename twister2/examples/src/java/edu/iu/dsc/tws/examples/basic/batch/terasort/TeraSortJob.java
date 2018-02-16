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
package edu.iu.dsc.tws.examples.basic.batch.terasort;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;

public final class TeraSortJob {
  private static final Logger LOG = Logger.getLogger(TeraSortJob.class.getName());

  private TeraSortJob() {
  }

  public static void main(String[] args) {
    Options options = new Options();
    options.addOption("input", true, "Input directory");
    options.addOption("output", true, "Output directory");
    options.addOption("partitionSampleNodes", true, "Number of nodes to choose partition samples");
    options.addOption("partitionSamplesPerNode",
        true, "Number of samples to choose from each node");
    options.addOption("filePrefix", true, "Prefix of the file partition");
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = commandLineParser.parse(options, args);
    } catch (ParseException e) {
      LOG.log(Level.SEVERE, "Failed to read the options", e);
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("program", options);
      throw new RuntimeException(e);
    }

    Map<String, Object> inputs = new HashMap<>();
    inputs.put("input", cmd.getOptionValue("input"));
    inputs.put("output", cmd.getOptionValue("output"));
    inputs.put("partitionSampleNodes",
        Integer.parseInt(cmd.getOptionValue("partitionSampleNodes")));
    inputs.put("partitionSamplesPerNode",
        Integer.parseInt(cmd.getOptionValue("partitionSamplesPerNode")));
    inputs.put("filePrefix", cmd.getOptionValue("filePrefix"));
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(inputs);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();


    BasicJob.BasicJobBuilder jobBuilder = BasicJob.newBuilder();
    jobBuilder.setName("terasort");
    jobBuilder.setContainerClass(TeraSortContainer.class.getName());
    jobBuilder.setRequestResource(new ResourceContainer(2, 1024), 12);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitContainerJob(jobBuilder.build(), config);
  }
}

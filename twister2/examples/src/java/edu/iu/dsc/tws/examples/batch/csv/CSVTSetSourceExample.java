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
package edu.iu.dsc.tws.examples.batch.csv;

import java.io.Serializable;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.fn.impl.CSVBasedSourceFunction;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;

public class CSVTSetSourceExample implements BatchTSetIWorker, Serializable {

  private static final Logger LOG = Logger.getLogger(CSVTSetSourceExample.class.getName());

  @Override
  public void execute(BatchTSetEnvironment env) {
    /*SourceTSet<String> lines = env.createSource("hello", new CSVSource("/tmp/dinput0"), 2);*/
    SourceTSet<Object> lines = env.createSource(new CSVBasedSourceFunction(
        "/tmp/dinput0", 100), 2);
    LOG.info("retrieved input lines are:" + lines);
    lines.direct().map((MapFunc<Integer[], Object>) i -> null).direct().forEach(
        i -> LOG.info("out" + Arrays.deepToString(i)));

    /*lines.direct().map((MapFunc<Integer[], String[]>) i -> null).direct().forEach(i -> LOG.info(
        "out" + Arrays.toString(i)));*/
  }

  public static void main(String[] args) throws Exception {
    LOG.log(Level.INFO, "Starting wordcount Job");

    Options options = new Options();
    options.addOption("parallelism", true, "Parallelism");

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();

    JobConfig jobConfig = new JobConfig();
    jobBuilder.setJobName("csvtest");
    jobBuilder.setWorkerClass(CSVTSetSourceExample.class);
    jobBuilder.addComputeResource(1, 512, 2);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), ResourceAllocator.getDefaultConfig());
  }
}

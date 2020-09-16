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
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.resource.Twister2Worker;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;

public class CSVTSetSourceExample implements Twister2Worker, Serializable {

  private static final Logger LOG = Logger.getLogger(CSVTSetSourceExample.class.getName());

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
    int dsize = 100;
    int parallelism = 2;
    int dimension = 2;
    SourceTSet<String[]> pointSource = env.createCSVSource("/tmp/dinput", dsize,
        parallelism, "split");
    ComputeTSet<double[][]> points = pointSource.direct().compute(
        new ComputeFunc<Iterator<String[]>, double[][]>() {
          private double[][] localPoints = new double[dsize / parallelism][dimension];

          @Override
          public double[][] compute(Iterator<String[]> input) {
            for (int i = 0; i < dsize / parallelism && input.hasNext(); i++) {
              String[] value = input.next();
              for (int j = 0; j < value.length; j++) {
                localPoints[i][j] = Double.parseDouble(value[j]);
              }
            }
            LOG.info("Double Array Values:" + Arrays.deepToString(localPoints));
            return localPoints;
          }
        });
  }

  public static void main(String[] args) throws Exception {
    LOG.log(Level.INFO, "Starting CSV Source Job");

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

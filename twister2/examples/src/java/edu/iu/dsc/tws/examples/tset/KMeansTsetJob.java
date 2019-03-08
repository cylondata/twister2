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
package edu.iu.dsc.tws.examples.tset;

import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.CachedTSet;
import edu.iu.dsc.tws.api.tset.MapFunction;
import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSet;
import edu.iu.dsc.tws.api.tset.TSetBaseWorker;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.examples.batch.kmeansoptimization.KMeansDataGenerator;
import edu.iu.dsc.tws.examples.batch.kmeansoptimization.KMeansJobParameters;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class KMeansTsetJob extends TSetBaseWorker implements Serializable {
  private static final Logger LOG = Logger.getLogger(KMeansTsetJob.class.getName());

  @Override
  public void execute(TSetEnv executionEnv) {
    LOG.log(Level.INFO, "TSet worker starting: " + workerId);

    /**
     * Setting execution mode
     */
    executionEnv.setMode(OperationMode.BATCH);

    KMeansJobParameters kMeansJobParameters = KMeansJobParameters.build(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int dsize = kMeansJobParameters.getDsize();
    int csize = kMeansJobParameters.getCsize();
    int iterations = kMeansJobParameters.getIterations();

    String dinputDirectory = kMeansJobParameters.getDatapointDirectory();
    String cinputDirectory = kMeansJobParameters.getCentroidDirectory();

    if (workerId == 0) {
      try {
        KMeansDataGenerator.generateData(
            "txt", new Path(dinputDirectory), numFiles, dsize, 100, dimension, config);
        KMeansDataGenerator.generateData(
            "txt", new Path(cinputDirectory), numFiles, csize, 100, dimension, config);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to create input data:", ioe);
      }
    }

    TSet<double[][]> points = executionEnv.createSource(new PointsSource()).cache();
    TSet<double[][]> centers = executionEnv.createSource(new CenterSource()).cache();

    for (int i = 0; i < iterations; i++) {
      TSet<double[][]> kmeansTSet = ((CachedTSet<double[][]>) points).map(new KMeansMap());
      kmeansTSet.addInput("centers", centers);

    }
  }

  public class KMeansMap implements MapFunction<double[][], double[][]> {

    @Override
    public double[][] map(double[][] doubles) {
      Object centers = CONTEXT.getInput("");

      return new double[0][];
    }
  }

  public class PointsSource implements Source<double[][]> {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public double[][] next() {
      return new double[0][];
    }
  }


  public class CenterSource implements Source<double[][]> {

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public double[][] next() {
      return new double[0][];
    }
  }
}


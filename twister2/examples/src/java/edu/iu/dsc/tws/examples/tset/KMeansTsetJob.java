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
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.BaseMapFunction;
import edu.iu.dsc.tws.api.tset.BaseSource;
import edu.iu.dsc.tws.api.tset.TSetBatchWorker;
import edu.iu.dsc.tws.api.tset.TwisterBatchContext;
import edu.iu.dsc.tws.api.tset.link.AllReduceTLink;
import edu.iu.dsc.tws.api.tset.sets.CachedTSet;
import edu.iu.dsc.tws.api.tset.sets.MapTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.formatters.LocalCompleteTextInputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.LocalFixedInputPartitioner;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansCalculator;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataGenerator;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerParameters;

public class KMeansTsetJob extends TSetBatchWorker implements Serializable {
  private static final Logger LOG = Logger.getLogger(KMeansTsetJob.class.getName());

  @Override
  public void execute(TwisterBatchContext tc) {
    LOG.log(Level.INFO, "TSet worker starting: " + workerId);

    KMeansWorkerParameters kMeansJobParameters = KMeansWorkerParameters.build(config);

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

    //TODO: consider what happens when same execEnv is used to create multiple graphs
    CachedTSet<double[][]> points = tc.createSource(
        new PointsSource(), parallelismValue).setName("dataSource").cache();
    CachedTSet<double[][]> centers = tc.createSource(
        new CenterSource(), parallelismValue).cache();


    for (int i = 0; i < iterations; i++) {
      MapTSet<double[][], double[][]> kmeansTSet = points.map(new KMeansMap());
      kmeansTSet.addInput("centers", centers);
      AllReduceTLink<double[][]> reduced = kmeansTSet.allReduce((t1, t2) -> {
        double[][] newCentroids = new double[t1.length]
            [t1[0].length];
        for (int j = 0; j < t1.length; j++) {
          for (int k = 0; k < t1[0].length; k++) {
            double newVal = t1[j][k] + t2[j][k];
            newCentroids[j][k] = newVal;
          }
        }
        return newCentroids;
      });
      centers = reduced.map(new AverageCenters(), parallelismValue).cache();
    }

    LOG.info("Final Centroids After\t" + iterations + "\titerations\t"
        + Arrays.deepToString(centers.getData().get(0)));
  }

  public class KMeansMap extends BaseMapFunction<double[][], double[][]> {
    private int dimension;

    @Override
    public double[][] map(double[][] doubles) {
      //TODO: cast needed since the context inputmap can hold many types of TSets, Solution?
      double[][] centers = (double[][]) context.getInput("centers").
          getPartitionData(context.getIndex());
      KMeansCalculator kMeansCalculator = new KMeansCalculator(doubles, centers, dimension);
      double[][] kMeansCenters = kMeansCalculator.calculate();
      return kMeansCenters;
    }

    @Override
    public void prepare() {
      Config cfg = context.getConfig();
      this.dimension = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DIMENSIONS));

    }
  }

  private class AverageCenters extends BaseMapFunction<double[][], double[][]> {

    @Override
    public double[][] map(double[][] centers) {
      LOG.log(Level.FINE, "Received centroids: " + context.getWorkerId()
          + ":" + context.getIndex());
      //The centers that are received at this map is a the sum of all points assigned to each
      //center and the number of points as the next element. So if the centers are 2D points
      //each entry will have 3 doubles where the last double is number of points assigned to
      //that center
      int dim = centers[0].length - 1;
      double[][] newCentroids = new double[centers.length][dim];
      for (int i = 0; i < centers.length; i++) {
        for (int j = 0; j < dim; j++) {
          double newVal = centers[i][j] / centers[i][dim];
          newCentroids[i][j] = newVal;
        }
      }
      return newCentroids;
    }

    @Override
    public void prepare() {

    }
  }

  public class PointsSource extends BaseSource<double[][]> {


    private DataSource<double[][], InputSplit<double[][]>> source;
    private int dataSize;
    private int dimension;
    private double[][] localPoints;
    private boolean read = false;

    @Override
    public void prepare() {
      LOG.info("Context Prepare Data Task Index:" + context.getIndex());

      int para = context.getParallelism();
      Config cfg = context.getConfig();
      this.dataSize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DSIZE));
      this.dimension = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DIMENSIONS));
      String datainputDirectory = cfg.getStringValue(DataObjectConstants.DINPUT_DIRECTORY);
      int datasize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DSIZE));
      //The +1 in the array size is because of a data balancing bug
      localPoints = new double[dataSize / para + 1][dimension];
      this.source = new DataSource(cfg, new LocalFixedInputPartitioner(new
          Path(datainputDirectory), context.getParallelism(), cfg, datasize),
          context.getParallelism());
    }

    @Override
    public boolean hasNext() {
      if (!read) {
        read = true;
        return true;
      }
      return false;
    }

    @Override
    public double[][] next() {
      LOG.fine("Context Prepare Center Task Index:" + context.getIndex());
      InputSplit inputSplit = source.getNextSplit(context.getIndex());
      int totalCount = 0;
      while (inputSplit != null) {
        try {
          int count = 0;
          while (!inputSplit.reachedEnd()) {
            String value = (String) inputSplit.nextRecord(null);
            if (value == null) {
              break;
            }
            String[] splts = value.split(",");
            for (int i = 0; i < dimension; i++) {
              localPoints[count][i] = Double.valueOf(splts[i]);
            }
            if (value != null) {
              count += 1;
            }
          }
          inputSplit = source.getNextSplit(context.getIndex());
        } catch (IOException e) {
          LOG.log(Level.SEVERE, "Failed to read the input", e);
        }
      }
      return localPoints;
    }
  }


  public class CenterSource extends BaseSource<double[][]> {

    private DataSource<double[][], InputSplit<double[][]>> source;
    private boolean read = false;
    private int dimension;
    private double[][] centers;

    @Override
    public void prepare() {
      Config cfg = context.getConfig();
      String datainputDirectory = cfg.getStringValue(DataObjectConstants.CINPUT_DIRECTORY);
      this.dimension = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DIMENSIONS));
      int csize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.CSIZE));

      this.centers = new double[csize][dimension];
      this.source = new DataSource(cfg, new LocalCompleteTextInputPartitioner(new
          Path(datainputDirectory), context.getParallelism(), cfg),
          context.getParallelism());
    }

    @Override
    public boolean hasNext() {
      if (!read) {
        read = true;
        return true;
      }
      return false;
    }

    @Override
    public double[][] next() {
      LOG.fine("Context Task Index:" + context.getIndex());
      InputSplit inputSplit = source.getNextSplit(context.getIndex());
      int totalCount = 0;
      while (inputSplit != null) {
        try {
          int count = 0;
          while (!inputSplit.reachedEnd()) {
            String value = (String) inputSplit.nextRecord(null);
            if (value == null) {
              break;
            }
            String[] splts = value.split(",");
            for (int i = 0; i < dimension; i++) {
              centers[count][i] = Double.valueOf(splts[i]);
            }
            if (value != null) {
              count += 1;
            }
          }
          LOG.info(context.getIndex() + " Counts : " + count);
          inputSplit = source.getNextSplit(context.getIndex());
        } catch (IOException e) {
          LOG.log(Level.SEVERE, "Failed to read the input", e);
        }
      }
      return centers;
    }


  }
}


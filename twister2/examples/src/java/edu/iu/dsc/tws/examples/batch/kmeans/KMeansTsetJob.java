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

package edu.iu.dsc.tws.examples.batch.kmeans;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.BaseMapFunc;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.api.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.api.tset.worker.BatchTSetIWorker;
import edu.iu.dsc.tws.data.api.formatters.LocalCompleteTextInputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.LocalFixedInputPartitioner;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.dataset.DataSource;

// TODO: this needs to checked for correctness!!!

public class KMeansTsetJob implements BatchTSetIWorker, Serializable {
  private static final Logger LOG = Logger.getLogger(KMeansTsetJob.class.getName());

  @Override
  public void execute(BatchTSetEnvironment tc) {
    int workerId = tc.getWorkerID();
    LOG.info("TSet worker starting: " + workerId);

    KMeansWorkerParameters kMeansJobParameters = KMeansWorkerParameters.build(tc.getConfig());
    KMeansWorkerUtils workerUtils = new KMeansWorkerUtils(tc.getConfig());

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int dsize = kMeansJobParameters.getDsize();
    int csize = kMeansJobParameters.getCsize();
    int iterations = kMeansJobParameters.getIterations();

    String dataDirectory = kMeansJobParameters.getDatapointDirectory();
    String centroidDirectory = kMeansJobParameters.getCentroidDirectory();

//    workerUtils.generateDatapoints(dimension, numFiles, dsize, csize, dataDirectory,
//        centroidDirectory);

    long startTime = System.currentTimeMillis();
    CachedTSet<double[][]> points =
        tc.createSource(new PointsSource(), parallelismValue).setName("dataSource").cache(false);
    CachedTSet<double[][]> centers =
        tc.createSource(new CenterSource(), parallelismValue).cache(false);

    long endTimeData = System.currentTimeMillis();
    ComputeTSet<double[][], Iterator<double[][]>> kmeansTSet =
        points.direct().map(new KMeansMap());

    ComputeTSet<double[][], double[][]> reduced = kmeansTSet.allReduce((ReduceFunc<double[][]>)
        (t1, t2) -> {
          double[][] newCentroids = new double[t1.length]
              [t1[0].length];
          for (int j = 0; j < t1.length; j++) {
            for (int k = 0; k < t1[0].length; k++) {
              double newVal = t1[j][k] + t2[j][k];
              newCentroids[j][k] = newVal;
            }
          }
          return newCentroids;
        }).map(new AverageCenters());

    for (int i = 0; i < iterations; i++) {
      kmeansTSet.addInput("centers", centers);
      centers = reduced.cache(true);
    }
    //reduced.finishIter();

    DataPartition<?> centroidPartition = centers.getDataObject().getPartition(workerId);
    double[][] centroid = null;
    if (centroidPartition.getConsumer().hasNext()) {
      centroid = (double[][]) centroidPartition.getConsumer().next();
    }

    long endTime = System.currentTimeMillis();
    if (workerId == 0) {
      LOG.info("Data Load time : " + (endTimeData - startTime) + "\n"
          + "Total Time : " + (endTime - startTime)
          + "Compute Time : " + (endTime - endTimeData));
      LOG.info("Final Centroids After\t" + iterations + "\titerations\t"
          + centroid.length);
    }
  }


  private class KMeansMap extends BaseMapFunc<double[][], double[][]> {
    private int dimension;

    @Override
    public void prepare(TSetContext context) {
      super.prepare(context);
      Config cfg = context.getConfig();
      this.dimension = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DIMENSIONS));

    }

    @Override
    public double[][] map(double[][] data) {
      double[][] centers = (double[][]) getTSetContext().getInput("centers").
          getPartition(getTSetContext().getIndex()).getConsumer().next();
      KMeansCalculator kMeansCalculator = new KMeansCalculator(data, centers, dimension);
      return kMeansCalculator.calculate();
    }
  }


  private class AverageCenters implements MapFunc<double[][], double[][]> {
    @Override
    public double[][] map(double[][] centers) {
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
  }


  private class PointsSource extends BaseSourceFunc<double[][]> {
    private DataSource<double[][], InputSplit<double[][]>> source;
    private int dataSize;
    private int dimension;
    private double[][] localPoints;
    private boolean read = false;

    @Override
    public void prepare(TSetContext context) {
      super.prepare(context);

      int para = context.getParallelism();
      Config cfg = context.getConfig();
      this.dataSize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DSIZE));
      this.dimension = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DIMENSIONS));
      String datainputDirectory = cfg.getStringValue(DataObjectConstants.DINPUT_DIRECTORY);
      int datasize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DSIZE));
      //The +1 in the array size is because of a data balancing bug
      localPoints = new double[dataSize / para][dimension];
      this.source = new DataSource(cfg, new LocalFixedInputPartitioner(new
          Path(datainputDirectory), context.getParallelism(), cfg, datasize),
          context.getParallelism());
    }

    @Override
    public boolean hasNext() {
      return !read;
    }

    @Override
    public double[][] next() {
      InputSplit inputSplit = source.getNextSplit(getTSetContext().getIndex());
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
            count += 1;
          }
          inputSplit = source.getNextSplit(getTSetContext().getIndex());
        } catch (IOException e) {
          LOG.log(Level.SEVERE, "Failed to read the input", e);
        }
      }

      read = true;
      return localPoints;
    }
  }


  public class CenterSource extends BaseSourceFunc<double[][]> {
    private DataSource<double[][], InputSplit<double[][]>> source;
    private boolean read = false;
    private int dimension;
    private double[][] centers;

    @Override
    public void prepare(TSetContext context) {
      super.prepare(context);

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
      InputSplit inputSplit = source.getNextSplit(getTSetContext().getIndex());
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
            count += 1;
          }
          inputSplit = source.getNextSplit(getTSetContext().getIndex());
        } catch (IOException e) {
          LOG.log(Level.SEVERE, "Failed to read the input", e);
        }
      }
      return centers;
    }
  }
}


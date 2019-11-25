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
package edu.iu.dsc.tws.examples.batch.cdfw;

import java.io.IOException;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataGenerator;

public class DataGeneratorSource extends BaseSource {

  private static final Logger LOG = Logger.getLogger(DataGeneratorSource.class.getName());

  private String edge;
  private int dim;
  private String dataDirectory;
  private String centroidDirectory;
  private int dsize;
  private int csize;
  private String byteType;

  public DataGeneratorSource() {
  }

  public DataGeneratorSource(String twister2DirectEdge, int dataSize, int centroidSize,
                             int dimension, String datadirectory, String centroiddirectory) {
    this.edge = twister2DirectEdge;
    this.dsize = dataSize;
    this.csize = centroidSize;
    this.dim = dimension;
    this.dataDirectory = datadirectory;
    this.centroidDirectory = centroiddirectory;
  }

  @Override
  public void execute() {
    generateData();
    context.writeEnd(edge, "Data Generation Finished");
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
  }

  public void generateData() {
    try {
      int numOfFiles = 1;
      int sizeMargin = 100;
      KMeansDataGenerator.generateData("txt", new Path(dataDirectory), numOfFiles, dsize,
          sizeMargin, dim, config);
      KMeansDataGenerator.generateData("txt", new Path(centroidDirectory), numOfFiles, csize,
          sizeMargin, dim, config);
    } catch (IOException ioe) {
      throw new Twister2RuntimeException("Failed to create input data:", ioe);
    }
  }
}

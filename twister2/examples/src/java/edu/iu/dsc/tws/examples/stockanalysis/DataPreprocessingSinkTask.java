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
package edu.iu.dsc.tws.examples.stockanalysis;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.IMessage;

public class DataPreprocessingSinkTask  extends BaseSink implements Collector {

  private static final Logger LOG = Logger.getLogger(DataPreprocessingSinkTask.class.getName());

  private String vectorDirectory;
  private String distanceDirectory;
  private String distanceType;

  public DataPreprocessingSinkTask(String vectordirectory, String distancedirectory,
                                   String distancetype) {
    this.vectorDirectory = vectordirectory;
    this.distanceDirectory = distancedirectory;
    this.distanceType = distancetype;
  }

  @Override
  public DataPartition<?> get() {
    return null;
  }

  @Override
  public boolean execute(IMessage content) {
    LOG.info("Received message:" + content.getContent().toString());
    //DistanceCalculator distanceCalculator = new DistanceCalculator(vectorDirectory,
    // distanceDirectory,
    //        Integer.parseInt(distanceType));
    //distanceCalculator.process();
    return false;
  }
}

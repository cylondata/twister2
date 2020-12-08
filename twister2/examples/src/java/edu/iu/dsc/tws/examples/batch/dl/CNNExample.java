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
package edu.iu.dsc.tws.examples.batch.dl;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.Twister2Worker;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.dl.data.MiniBatch;
import edu.iu.dsc.tws.dl.data.dataset.DataSet;
import edu.iu.dsc.tws.dl.data.dataset.DataSetFactory;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;

import java.io.Serializable;

public class CNNExample implements Twister2Worker, Serializable {

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    long startTime = System.nanoTime();

    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
    Config config = env.getConfig();
    int parallelism = config.getIntegerValue("parallelism");
    int dataSize = config.getIntegerValue("dataSize");
    int batchSize = config.getIntegerValue("batchSize");
    int epoch = config.getIntegerValue("epoch");
    String dataFile = config.getStringValue("data");
    if (batchSize % parallelism != 0) {
      throw new IllegalStateException("batch size should be a multiple of parallelism");
    }
    int miniBatchSize = batchSize / parallelism;
    int imageSize = 0;
    DataSet<MiniBatch> source = DataSetFactory.createImageMiniBathDataSet(env, dataFile, imageSize,
        miniBatchSize, dataSize, parallelism);
    source.transform(null);
  }
}

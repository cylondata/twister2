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
package edu.iu.dsc.tws.examples.batch.kmeansoptimization;

import java.io.IOException;

import org.junit.Test;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.data.fs.Path;

public class KMeansDataGeneratorTest {

  @Test
  public void testUniqueSchedules() throws IOException {
    int parallel = 2;
    Config config = getConfig();
    String dinputDirectory = "/tmp/dinput";
    int numFiles = 1;
    int dsize = 100;
    int dimension = 2;
    String fileSys = "local";

    KMeansDataGenerator.generateData("txt", new Path(dinputDirectory),
        numFiles, dsize, 100, dimension, fileSys, config);
  }

  private Config getConfig() {

    String twister2Home = "/home/kannan/twister2/bazel-bin/scripts/package/twister2-0.1.0";
    String configDir = "/home/kannan/twister2/twister2/taskscheduler/tests/conf/";
    String clusterType = "standalone";

    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);
    return Config.newBuilder().putAll(config).build();
  }
}

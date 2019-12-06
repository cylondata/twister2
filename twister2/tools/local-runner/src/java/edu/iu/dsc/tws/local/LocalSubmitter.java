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
package edu.iu.dsc.tws.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.local.mock.MockWorker;
import edu.iu.dsc.tws.local.util.LocalClassLoader;

public final class LocalSubmitter {

  private static final Logger LOG = Logger.getLogger(LocalSubmitter.class.getName());

  /**
   * This method should be called in main method before calling any other twister2 api method
   */
  public static LocalSubmitter prepare(String configDir) {
    System.setProperty("cluster_type", "standalone");

    // do a simple config dir validation
    File cDir = new File(configDir, "standalone");

    String[] filesList = new String[]{
        "core.yaml",
        "network.yaml",
        "data.yaml",
        "resource.yaml",
        "task.yaml",
    };

    for (String file : filesList) {
      File toCheck = new File(cDir, file);
      if (!toCheck.exists()) {
        throw new Twister2RuntimeException("Couldn't find " + file
            + " in config directory specified.");
      }
    }

    System.setProperty("config_dir", configDir);
    System.setProperty("twister2_home", "/tmp");

    // setup logging
    try {
      File commonConfig = new File(configDir, "common");
      FileInputStream fis = new FileInputStream(new File(commonConfig, "logger.properties"));
      LogManager.getLogManager().readConfiguration(fis);
    } catch (IOException e) {
      LOG.warning("Couldn't load logging configuration");
    }

    return new LocalSubmitter();
  }

  public LocalSubmitter withTwsHome(String twsHome) {
    System.setProperty("twister2_home", twsHome);
    return this;
  }

  public void submitJob(Twister2Job twister2Job, Config config) {
    Config newConfig = overrideConfigs(config);

    CyclicBarrier cyclicBarrier = new CyclicBarrier(twister2Job.getNumberOfWorkers());

    for (int i = 0; i < twister2Job.getNumberOfWorkers(); i++) {
      startWorker(twister2Job, newConfig, i, cyclicBarrier);
    }
  }

  /**
   * This methods override some configs to suite a local running environment
   */
  private Config overrideConfigs(Config config) {
    return Config.newBuilder()
        .putAll(config)
        .put("twister2.network.channel.class", "edu.iu.dsc.tws.comms.tcp.TWSTCPChannel")
        .put("twister2.job.master.used", false)
        .put("twister2.checkpointing.enable", false)
        .build();
  }

  /**
   * This method starts a new worker instance on a separate thread.
   */
  private void startWorker(Twister2Job twister2Job,
                           Config config, int workerId, CyclicBarrier cyclicBarrier) {
    LocalClassLoader localClassLoader = new LocalClassLoader(LocalSubmitter.class.getClassLoader());
    localClassLoader.addJobClass(twister2Job.getWorkerClass());
    try {
      Object o = localClassLoader.loadClass(MockWorker.class.getName())
          .getConstructor(
              twister2Job.getClass(),
              config.getClass(),
              Integer.class,
              CyclicBarrier.class
          ).newInstance(twister2Job, config, workerId, cyclicBarrier);
      Thread thread = new Thread(
          (Runnable) o
      );
      thread.setName("worker-" + workerId);
      thread.start();
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | InstantiationException
        | InvocationTargetException e) {
      e.printStackTrace();
    }
  }
}

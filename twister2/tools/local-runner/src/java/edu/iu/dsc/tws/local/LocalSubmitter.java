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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.driver.DriverJobState;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.scheduler.Twister2JobState;
import edu.iu.dsc.tws.local.mock.MockWorker;
import edu.iu.dsc.tws.local.util.LocalClassLoader;

public final class LocalSubmitter {

  private static final Logger LOG = Logger.getLogger(LocalSubmitter.class.getName());

  private static boolean prepared = false;
  private static boolean failed = false;
  private static Throwable fault;

  private static final String[] FILES_LIST = new String[]{
      "core.yaml",
      "network.yaml",
      "data.yaml",
      "resource.yaml",
      "task.yaml",
  };

  private LocalSubmitter() {

  }

  /**
   * This method sets the necessary initial configurations to execute the twister2 core classes
   */
  private static LocalSubmitter prepare(String configDir) {
    System.setProperty("cluster_type", "standalone");

    // do a simple config dir validation
    File cDir = new File(configDir, "standalone");

    for (String file : FILES_LIST) {
      File toCheck = new File(cDir, file);
      if (!toCheck.exists()) {
        throw new Twister2RuntimeException("Couldn't find " + file
            + " in config directory specified.");
      }
    }

    System.setProperty("config_dir", configDir);
    System.setProperty("twister2_home", System.getProperty("java.io.tmpdir"));

    // setup logging
    try {
      File commonConfig = new File(configDir, "common");
      FileInputStream fis = new FileInputStream(new File(commonConfig, "logger.properties"));
      LogManager.getLogManager().readConfiguration(fis);
    } catch (IOException e) {
      LOG.warning("Couldn't load logging configuration");
    }

    prepared = true;

    return new LocalSubmitter();
  }

  /**
   * This method will create a mock config dir and call {@link LocalSubmitter#prepare(String)}
   */
  private static LocalSubmitter prepare() {
    try {
      File tempDir = Files.createTempDirectory(UUID.randomUUID().toString()).toFile();

      //create standalone and common directory
      File commonConfig = new File(tempDir, "common");
      File standaloneConfig = new File(tempDir, "standalone");

      List<File> directories = new ArrayList<>();
      directories.add(commonConfig);
      directories.add(standaloneConfig);

      List<File> files = new ArrayList<>();
      for (String f : FILES_LIST) {
        files.add(new File(commonConfig, f));
        files.add(new File(standaloneConfig, f));
      }
      files.add(new File(commonConfig, "logger.properties"));

      directories.forEach(File::mkdir);
      for (File file : files) {
        file.createNewFile();
      }
      return prepare(tempDir.getAbsolutePath());
    } catch (IOException e) {
      throw new Twister2RuntimeException("Failed to create a mock config directory");
    }
  }

  /**
   * This method can be used to run a {@link Twister2Job} with default configurations
   */
  public static Twister2JobState submitJob(Twister2Job twister2Job) {
    return submitJob(twister2Job, Config.newBuilder().build());
  }

  /**
   * This method can be used to submit a {@link Twister2Job}.
   * Additional configurations can be loaded by specifying the root of a twister2 configuration
   * directory. Configurations loaded from the files can be overridden in {@link Config} object.
   */
  public static Twister2JobState submitJob(Twister2Job twister2Job,
                                           String configDir, Config config) {
    prepare(configDir);
    return submitJob(twister2Job, config);
  }

  /**
   * This method can be used to submit a {@link Twister2Job}.
   * Additional configurations can be loaded by specifying the root of a twister2 configuration
   * directory.
   */
  public static Twister2JobState submitJob(Twister2Job twister2Job, String configDir) {
    prepare(configDir);
    return submitJob(twister2Job);
  }

  /**
   * This method can be used to run a {@link Twister2Job} with default configurations.
   * Additional configurations can be defined/overridden by passing the {@link Config} object.
   */
  public static Twister2JobState submitJob(Twister2Job twister2Job, Config config) {
    Twister2JobState state = new Twister2JobState(false);
    if (!prepared) {
      prepare();
    }

    Config newConfig = overrideConfigs(config);

    CyclicBarrier cyclicBarrier = new CyclicBarrier(twister2Job.getNumberOfWorkers());

    for (int i = 0; i < twister2Job.getNumberOfWorkers(); i++) {
      startWorker(twister2Job, newConfig, i, cyclicBarrier);
    }
    if (failed) {
      state.setJobstate(DriverJobState.FAILED);
      state.setCause((Exception) fault);
    } else {
      state.setJobstate(DriverJobState.COMPLETED);
    }
    //reset the local state for next job
    failed = false;
    fault =  null;
    return state;
  }

  /**
   * This methods override some configs to suite a local running environment
   */
  private static Config overrideConfigs(Config config) {
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
  private static void startWorker(Twister2Job twister2Job,
                                  Config config, int workerId, CyclicBarrier cyclicBarrier) {
    Thread.UncaughtExceptionHandler hndler = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        failed = true;
        fault = ex;
        return;
      }
    };
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
      thread.setUncaughtExceptionHandler(hndler);
      thread.start();
      thread.join();
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | InstantiationException
        | InvocationTargetException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

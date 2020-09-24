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

package edu.iu.dsc.tws.api.resource;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.faulttolerance.JobProgress;
import edu.iu.dsc.tws.api.util.CommonThreadPool;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * Worker environment encapsulating the details about the workers.
 */
public final class WorkerEnvironment {
  private static final Logger LOG = Logger.getLogger(WorkerEnvironment.class.getName());

  /**
   * Configuration of the worker
   */
  private Config config;

  /**
   * Worker id
   */
  private int workerId;

  /**
   * Job object
   */
  private JobAPI.Job job;

  /**
   * Worker controller
   */
  private IWorkerController workerController;

  /**
   * Persistent storage
   */
  private IPersistentVolume persistentVolume;

  /**
   * Volatile storage, this will be erased after job finished
   */
  private IVolatileVolume volatileVolume;

  /**
   * The communication channel
   */
  private Communicator communicator;

  /**
   * The underlying channel
   */
  private TWSChannel channel;

  /**
   * The worker list we got from discovery
   */
  private final List<JobMasterAPI.WorkerInfo> workerList;

  /**
   * keep track of worker reExecutions when initializing
   * when init is called, if executeCount is smaller,
   * that means, worker is reexecuted.
   * So, some reinitializations need to be performed
   */
  private static int executeCount = 1;

  /**
   * Singleton environment
   */
  private static volatile WorkerEnvironment workerEnv;

  /**
   * This can be used to temporary hold runtime objects or as a medium to share runtime objects
   * between two disconnected classes
   */
  private static volatile Map<String, Object> sharedKeyValueStore = new HashMap<>();

  private WorkerEnvironment(Config config, JobAPI.Job job, IWorkerController workerController,
                            IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    this.config = config;
    this.workerId = workerController.getWorkerInfo().getWorkerID();
    this.job = job;
    this.workerController = workerController;
    this.persistentVolume = persistentVolume;
    this.volatileVolume = volatileVolume;

    //initialize common thread pool
    CommonThreadPool.init(config);

    //wait for the workers to join
    try {
      this.workerList = workerController.getAllWorkers();
    } catch (TimeoutException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      throw new RuntimeException("Unable to get the worker list", e);
    }

    // if this is a Kubernetes cluster, and it is not an OpenMPI job,
    // check whether all worker pods are reachable
    // sometimes it takes some time to populate dns ip values in Kubernetes
    // although all workers is started, some workers may be unreachable by ip address
    if (Context.isKubernetesCluster(config)
        && !SchedulerContext.usingOpenMPI(config)
        && SchedulerContext.checkPodsReachable(config)) {
      checkAllPodsReachable();
    }

    // create the channel
    this.channel = Network.initializeChannel(config, workerController);
    // create the communicator
    this.communicator = new Communicator(config, channel);
  }

  public Config getConfig() {
    return config;
  }

  public int getWorkerId() {
    return workerId;
  }

  public JobAPI.Job getJob() {
    return job;
  }

  public int getNumberOfWorkers() {
    return this.workerController.getNumberOfWorkers();
  }

  public List<JobMasterAPI.WorkerInfo> getWorkerList() {
    return this.workerList;
  }

  public IWorkerController getWorkerController() {
    return workerController;
  }

  public IPersistentVolume getPersistentVolume() {
    return persistentVolume;
  }

  public IVolatileVolume getVolatileVolume() {
    return volatileVolume;
  }

  public Communicator getCommunicator() {
    return communicator;
  }

  public TWSChannel getChannel() {
    return channel;
  }

  /**
   * Close the worker environment
   */
  public void close() {
    this.communicator.close();
    CommonThreadPool.close();
    this.workerEnv = null;
  }

  /**
   * TODO: this is in infinite loop if some pods are not reachable
   * we should put a limit to waiting and checking
   * TODO: we are checking reachability of all pods in the job
   * we should only check the pods this worker will connect to.
   */
  private boolean checkAllPodsReachable() {

    Set<String> podIPs = workerList
        .stream()
        .map(workerInfo -> workerInfo.getWorkerIP())
        .collect(Collectors.toSet());

    // remove own IP, no need to check own pod
    podIPs.removeIf(ipStr -> ipStr.equals(workerController.getWorkerInfo().getWorkerIP()));

    long start = System.currentTimeMillis();
    long count = 0;
    final long logPeriod = 6;
    final long sleepInterval = 500;

    while (!podIPs.isEmpty()) {
      podIPs.removeIf(Network::isReachable);

      if (podIPs.isEmpty()) {
        LOG.info("All worker pods are reachable by IP. count: " + count);
        return true;
      }

      try {
        Thread.sleep(sleepInterval);
      } catch (InterruptedException e) {
      }

      count++;
      if (count % logPeriod == 0) {
        long duration = (System.currentTimeMillis() - start) / 1000;
        LOG.info("We are trying to reach following worker pod IP addresses for " + duration
            + " seconds: " + String.join(", ", podIPs));
      }
    }

    return true;
  }

  /**
   * Initialize the worker environment, this is a singleton and every job should call this method
   *
   * @param config configuration
   * @param job job object for this Twister2 job
   * @param workerController worker controller
   * @param persistentVolume persistent volume
   * @param volatileVolume volatile volume
   * @return the worker environment
   */
  public static synchronized WorkerEnvironment init(Config config,
                                                    JobAPI.Job job,
                                                    IWorkerController workerController,
                                                    IPersistentVolume persistentVolume,
                                                    IVolatileVolume volatileVolume) {
    if (workerEnv == null) {
      workerEnv = new WorkerEnvironment(
          config, job, workerController, persistentVolume, volatileVolume);
    } else if (isWorkerReExecuting()) {
      // If the worker Env exists and execute count increased
      // reset the config (need to check if complete re-init is needed)
      workerEnv.setConfig(config);
      workerEnv.getCommunicator().reInit();
      workerEnv.getChannel().reInit(JobProgress.getRestartedWorkers());
    }
    return workerEnv;
  }

  /**
   * this method will return true only once for each re-execution
   * so, the singleton object will be created only for once
   */
  private static boolean isWorkerReExecuting() {
    boolean reExecuting = executeCount < JobProgress.getWorkerExecuteCount();
    executeCount = JobProgress.getWorkerExecuteCount();
    return reExecuting;
  }

  private void setConfig(Config conf) {
    this.config = conf;
  }

  /*Shared Key-Value Store Related Methods*/

  public static void putSharedValue(String key, Object value) {
    sharedKeyValueStore.put(key, value);
  }

  /**
   * This method will wrap the value with a {@link WeakReference} before inserting into the
   * key-value store.
   */
  public static void putWeakSharedValue(String key, Object value) {
    putSharedValue(key, new WeakReference<>(value));
  }

  public static Object removeSharedValue(String key) {
    return sharedKeyValueStore.remove(key);
  }

  public static Object getSharedValue(String key) {
    Object obj = sharedKeyValueStore.get(key);
    if (obj != null) {
      if (obj instanceof WeakReference) {
        return ((WeakReference) obj).get();
      } else {
        return obj;
      }
    }
    return null;
  }

  public static <T> T getSharedValue(String key, Class<T> clazz) {
    Object obj = getSharedValue(key);
    if (obj != null) {
      return clazz.cast(obj);
    }
    return null;
  }

  public static WorkerEnvironment getWorkerEnv() {
    return workerEnv;
  }
}

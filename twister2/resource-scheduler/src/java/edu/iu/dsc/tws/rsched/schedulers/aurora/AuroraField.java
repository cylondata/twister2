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

package edu.iu.dsc.tws.rsched.schedulers.aurora;

/**
 * Field names passed to aurora controllers during job creation
 */
public enum AuroraField {
  AURORA_CLUSTER_NAME,
  ENVIRONMENT,
  ROLE,
  JOB_NAME,
  CPUS_PER_WORKER,
  DISK_PER_WORKER,
  RAM_PER_WORKER,
  NUMBER_OF_WORKERS,
  TWISTER2_PACKAGES_PATH,
  CORE_PACKAGE_FILENAME,
  JOB_PACKAGE_FILENAME,
  JOB_PACKAGE_URI,
  JOB_DESCRIPTION_FILE,
  USER_JOB_JAR_FILE,
  CLUSTER_TYPE,
  AURORA_WORKER_CLASS
}

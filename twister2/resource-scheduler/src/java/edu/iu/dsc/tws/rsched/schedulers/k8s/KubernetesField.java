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
package edu.iu.dsc.tws.rsched.schedulers.k8s;

/**
 * Field names passed to KubernetesWorker when starting a worker in a container
 */
public enum KubernetesField {
  USER_JOB_JAR_FILE,    // java jar file for running user job
  JOB_PACKAGE_FILE_SIZE, // file size of tar.gz file
  CONTAINER_NAME,
  POD_IP
}

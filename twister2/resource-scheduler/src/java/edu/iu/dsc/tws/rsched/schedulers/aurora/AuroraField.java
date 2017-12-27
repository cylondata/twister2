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
  CLUSTER,
  ENVIRONMENT,
  ROLE,
  JOB_NAME,
  CPUS_PER_CONTAINER,
  DISK_PER_CONTAINER,
  RAM_PER_CONTAINER,
  NUMBER_OF_CONTAINERS,
  TWISTER2_PACKAGE_PATH,
  TWISTER2_PACKAGE_FILE
}

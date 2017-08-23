//  Copyright 2017 Twitter. All rights reserved.
//
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
package edu.iu.dsc.tws.rsched.schedulers.slurmmpi;

import mpi.MPI;
import mpi.MPIException;

public class MPIProcess {
  public static void main(String[] args) {
    try {
      MPI.Init(args);

      // know the location of the job jar file

      // it has to know the master

      // it has to start the executor - thread based - we have to give some configuration

      // it has to estabilish some form of connecttion task scheduler


      MPI.Finalize();
    } catch (MPIException e) {
      e.printStackTrace();
    }
  }
}

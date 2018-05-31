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
package edu.iu.dsc.tws.examples.basic;

import mpi.MPI;

public final class BasicMpiJob {

  private BasicMpiJob() {

  }

  public static void main(String[] args) {
    try {

      System.out.printf("inside openmpi: Openmpi has started");
      MPI.Init(args);
      int myrank = MPI.COMM_WORLD.getRank();
      int size = MPI.COMM_WORLD.getSize();
      System.out.println("Hello world from rank " + myrank + " of " + size);

      MPI.Finalize();
    } catch (Exception e) {
      e.printStackTrace();
    }


  }
}

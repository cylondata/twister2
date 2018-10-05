#!/usr/bin/env bash

mpirun -np $1 -hostfile $2 java -cp "lib/*" edu.iu.dsc.tws.rsched.schedulers.standalone.bootstrap.MPIBootstrap $3 $4 $5 $6 $7 $8
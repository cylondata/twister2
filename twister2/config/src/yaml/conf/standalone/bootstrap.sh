#!/usr/bin/env bash

# set the lib path to bundled mpi, if that mpirun is used
lib_path="-Djava.library.path=${LD_LIBRARY_PATH}"
if [ "${10}" = "${11}/ompi/bin/mpirun" ]; then
  lib_path="${lib_path}:${11}/ompi/lib"
fi

${10} -np $1 -hostfile $2 java $lib_path -cp "lib/*" -Djava.util.logging.config.file=common/logger.properties edu.iu.dsc.tws.rsched.schedulers.standalone.bootstrap.MPIBootstrap $3 $4 $5 $6 $7 $8 $9
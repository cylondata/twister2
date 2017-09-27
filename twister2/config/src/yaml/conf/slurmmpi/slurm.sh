#!/usr/bin/env bash

# arg1: the heron executable
# arg2: arguments to executable

#SBATCH --ntasks-per-node=1
#SBATCH --time=00:30:00

$BUILD/bin/mpirun --report-bindings --mca btl ^tcp java $opts -cp $cp -DNumberDataPoints=$2 -DDistanceMatrixFile=$1 -DPointsFile=$3.txt -DTimingFile=$4timing.txt -DSummaryFile=$4.summary.txt -DTransformationFunction=trfm.DistanceTransformer edu.indiana.soic.spidal.damds.Program -c config.properties -n $SLURM_JOB_NUM_NODES -t $tpn | tee $4.summary.txt

echo $SLURM_JOB_ID > slurm-job.pid

wait

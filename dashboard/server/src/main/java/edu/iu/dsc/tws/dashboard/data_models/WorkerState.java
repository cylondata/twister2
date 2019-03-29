package edu.iu.dsc.tws.dashboard.data_models;

public enum WorkerState {
  STARTING, RUNNING, COMPLETED,
  KILLED, NOT_PINGING, FAILED,
  KILLED_BY_SCALE_DOWN
}

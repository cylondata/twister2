package edu.iu.dsc.tws.dashboard.data_models;

public enum WorkerState {
  STARTED, RESTARTED, COMPLETED,
  KILLED, NOT_PINGING, FAILED,
  KILLED_BY_SCALE_DOWN
}

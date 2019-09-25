package edu.iu.dsc.tws.python.config;

public class ComputeResource {

  private double cpu;
  private int ram;
  private int instances = 1;

  public ComputeResource(double cpu, int ram, int instances) {
    this.cpu = cpu;
    this.ram = ram;
    this.instances = instances;
  }

  public double getCpu() {
    return cpu;
  }

  public int getRam() {
    return ram;
  }

  public int getInstances() {
    return instances;
  }
}

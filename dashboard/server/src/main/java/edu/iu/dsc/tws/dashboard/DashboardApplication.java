package edu.iu.dsc.tws.dashboard;

import org.springframework.boot.SpringApplication;

public final class DashboardApplication {

  private DashboardApplication() {

  }

  public static void main(String[] args) {
    SpringApplication.run(DashboardSpringConfig.class, args);
  }
}

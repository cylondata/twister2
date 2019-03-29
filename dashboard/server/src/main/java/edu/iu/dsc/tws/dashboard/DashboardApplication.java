package edu.iu.dsc.tws.dashboard;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;

public final class DashboardApplication {

  private static final Logger LOG = LogManager.getLogger(DashboardApplication.class);

  private DashboardApplication() {

  }

  public static void main(String[] args) throws IOException {
    LOG.info("Starting dashboard server...");
    SpringApplication.run(DashboardSpringConfig.class, args);
  }
}

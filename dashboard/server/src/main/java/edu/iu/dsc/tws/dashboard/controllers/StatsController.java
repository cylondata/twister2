package edu.iu.dsc.tws.dashboard.controllers;

import edu.iu.dsc.tws.dashboard.rest_models.ElementStatsResponse;
import edu.iu.dsc.tws.dashboard.services.StatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("stats")
public class StatsController {

  @Autowired
  private StatsService statsService;

  @RequestMapping(value = "/elements/", method = RequestMethod.GET)
  public ElementStatsResponse getElementCounts() {
    return statsService.getElementStats();
  }
}

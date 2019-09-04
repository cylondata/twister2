#include "example_worker.h"
#include "config/config.h"

#include <iostream>

void ExampleWorker::execute(twister2::api::config::Config *config, int workerId) {
  std::cout << "Hello, World!";
}

extern "C" twister2::api::resource::IWorker *twister2_iworker_create() {
  return new ExampleWorker();
}
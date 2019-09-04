#ifndef TWISTER2_EXAMPLE_WORKER_H
#define TWISTER2_EXAMPLE_WORKER_H

#include "resource/worker.h"
#include "config/config.h"

class ExampleWorker : public twister2::api::resource::IWorker {
public:
  void execute(twister2::api::config::Config *config, int worker_id);
};

#endif //TWISTER2_EXAMPLE_WORKER_H

#ifndef WORKER_H_
#define WORKER_H_

#include "../config/config.h"

namespace twister2 {
  namespace api {
    namespace resource {

      class IWorker {
      public:
        virtual void execute(twister2::api::config::Config *config, int worker_id);
      };

    }
  }
}

/**
 * The function to create the IWorker instance
 */
typedef twister2::api::resource::IWorker* twister2_iworker_create_t();
//twister2::api::resource::IWorker *twister2_iworker_create();

#endif


#ifndef TWISTER2_TWISTER2SUBMITTER_H
#define TWISTER2_TWISTER2SUBMITTER_H

#include "glog/logging.h"

#include "twister2_job.h"

namespace twister2 {
  namespace resource {
    namespace core {
      class Twister2Submitter {
      public:
        static void submit_job(twister2::api::Twister2Job *job);
      };
    }
  }
}


#endif //TWISTER2_TWISTER2SUBMITTER_H

#include "twister2_submitter.h"

namespace twister2 {
  namespace resource {
    namespace core {
      void Twister2Submitter::submit_job(twister2::api::Twister2Job *job) {
        // first serialize the job=
        void * buf;
        size_t length;
        if (twister2::api::Twister2Job::serialize_to_array(job->serialize(), &buf, &length)) {
          LOG(ERROR) << "Failed to serialize the job: " << job->get_job_name();
          return;
        }

        // now lets write the job to file

      }
    }
  }
}
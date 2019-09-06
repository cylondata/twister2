#include "twister2_job.h"
#include "proto/job.pb.h"

namespace twister2 {
  namespace api {
    tws::proto::job::Job * Twister2Job::serialize() {
      tws::proto::job::Job* job = new tws::proto::job::Job();
      job->set_job_name(_job_name);
      job->set_worker_class_name(_method_name);

      return job;
    }

    int Twister2Job::serialize_to_array(tws::proto::job::Job *job, void **buf, size_t *length) {
      *length = job->ByteSizeLong();
      *buf = malloc(*length);
      job->SerializeToArray(buf, *length);
      return 0;
    }
  }
}

#ifndef TWISTER2_TWISTER2JOB_H
#define TWISTER2_TWISTER2JOB_H

#include "config/config.h"
#include "proto/job.pb.h"

#include <string>

namespace twister2 {
  namespace api {
    class Twister2Job {
    public:
      void set_job_name(std::string _name) {
        _job_name = _name;
      }

      void set_method_name(std::string _name) {
        _method_name = _name;
      }

      void set_cfg(twister2::api::config::Config* _cfg) {
        _config = _cfg;
      }

      std::string get_job_name() {
        return _job_name;
      }

      twister2::api::config::Config* get_config() {
        return _config;
      }

      std::string get_method_name() {
        return _method_name;
      }

      /**
       * Serialize the job to a protocol buffer
       * @return a protobuf Job
       */
      tws::proto::job::Job* serialize();

      /**
       * Serialize the protocol buffer to a byte array
       *
       * @param job the protobuf object
       * @param buf
       * @param length
       * @return
       */
      static int serialize_to_array(tws::proto::job::Job* job, void ** buf, int * length);

    private:
      std::string _job_name;
      std::string _method_name;
      twister2::api::config::Config *_config;
    };
  }
}
#endif //TWISTER2_TWISTER2JOB_H

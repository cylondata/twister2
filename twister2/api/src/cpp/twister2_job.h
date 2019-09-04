#ifndef TWISTER2_TWISTER2JOB_H
#define TWISTER2_TWISTER2JOB_H

#include "config/config.h"

#include <string>

namespace twister2 {
  namespace api {
    class Twister2Job {
    public:
      void set_job_name(std::string _name) {
        _job_name = _name;
      }

      void set_dll_name(std::string _name) {
        _dll_name = _name;
      }

      void set_cfg(twister2::api::config::Config *_cfg) {
        _config = _cfg;
      }

      std::string get_job_name() {
        return _job_name;
      }

      std::string get_dll_name() {
        return _dll_name;
      }

      twister2::api::config::Config *get_config() {
        return _config;
      }

    private:
      std::string _job_name;
      std::string _dll_name;
      twister2::api::config::Config *_config;
    };
  }
}
#endif //TWISTER2_TWISTER2JOB_H

#include "resource-config-reader.h"
#include "glog/logging.h"

namespace twister2 {
  namespace config {
    twister2::config::ResourceConfigReader* twister2::config::ResourceConfigReader::instance_;

    ResourceConfigReader* twister2::config::ResourceConfigReader::getInstance() {
      if (instance_ == NULL) {
        LOG(ERROR) << "Need to call create before calling getInstance()";
      }
      return instance_;
    }

    int twister2::config::ResourceConfigReader::createInstance(std::string _file) {
      instance_ = new ResourceConfigReader(_file);
      if (instance_ == NULL) {
        LOG(ERROR) << "Cannot create the instance - out of memory";
        return 1;
      }
      return 0;
    }

    twister2::config::ResourceConfigReader::ResourceConfigReader(std::string _file)
      : reader_(_file){
      reader_.LoadConfig();
    }

    std::string twister2::config::ResourceConfigReader::get_twister2_job_file_location() {
      return (*(reader_.getNode()))[TWISTER2_JOB_FILE_LOCATION].as<std::string>();
    }
  }
}

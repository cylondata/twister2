#include "config/config-reader.h"
#include "glog/logging.h"

namespace twister2 {
namespace config {

ConfigReader::ConfigReader(const std::string& _file)
    : _file(_file) {
}

ConfigReader::~ConfigReader() {}

void ConfigReader::LoadConfig() {
  config_ = YAML::LoadFile(_file);
  if (config_.Type() == YAML::NodeType::Null) {
    config_[""] = "";
    LOG(INFO) << "Config file " << _file << " changed. reloading..";
  }
}

}  // namespace config
}
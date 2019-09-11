#ifndef CONFIG_READER_H_
#define CONFIG_READER_H_

#include "yaml-cpp/yaml.h"

namespace twister2 {
namespace config {

/**
 * Reads the YAML file
 */
class ConfigReader {
 public:
  ConfigReader(const std::string& _file);
  virtual ~ConfigReader();
  YAML::Node* getNode();
  void LoadConfig();

 private:
  YAML::Node config_;
  std::string _file;
};
}
}

#endif


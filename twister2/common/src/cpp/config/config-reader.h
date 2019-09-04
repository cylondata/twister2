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

 protected:
  void LoadConfig();

  YAML::Node config_;

 private:
  std::string _file;
};
}
}

#endif


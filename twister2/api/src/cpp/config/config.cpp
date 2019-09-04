#include "config.h"

namespace twister2 {
  namespace api {
    namespace config {
      void Config::set_key_value(std::string &key, std::string &val) {
        _config_map[key] = val;
      }
    }
  }
}

#ifndef CONFIG_H
#define CONFIG_H

#include <string>
#include <map>

namespace twister2 {
  namespace api {
    namespace config {
      /**
       * The configuration class
       */
      class Config {
      public:
        void set_key_value(std::string &key, std::string &val);

      private:
        std::map <std::string, std::string> _config_map;
      };

    }
  }
}

#endif
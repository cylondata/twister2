#ifndef TWISTER2_RESOURCE_CONFIG_READER_H
#define TWISTER2_RESOURCE_CONFIG_READER_H

#include <string>

#include "config-reader.h"

namespace twister2 {
  namespace config {
    class ResourceConfigReader {
    private:
      /**
       * Private contructor for singletor
       * @param _file
       */
      ResourceConfigReader(std::string _file);

      /**
       * The reader object
       */
      twister2::config::ConfigReader reader_;

      /*
       * The instance to return in singleton
       */
      static ResourceConfigReader* instance_;
    public:
      /**
       * Public constants for properties
       */
      std::string TWISTER2_JOB_FILE_LOCATION = "twister2.resource.job.file.dir";

      /**
       * Create instance
       * @param _file  file
       * @return
       */
      static int createInstance(std::string _file);

      static ResourceConfigReader* getInstance();

      std::string get_twister2_job_file_location();
    };
  }
}

#endif //TWISTER2_RESOURCE_CONFIG_READER_H

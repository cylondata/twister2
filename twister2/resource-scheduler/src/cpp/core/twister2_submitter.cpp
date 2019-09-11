#include <fstream>

#include "twister2_submitter.h"
#include "twister2_job.h"
#include "resource-config-reader.h"

namespace twister2 {
  namespace resource {
    namespace core {
      void Twister2Submitter::submit_job(twister2::api::Twister2Job *job) {
        // first serialize the job=
        void * buf;
        size_t length;
        if (twister2::api::Twister2Job::serialize_to_array(job->serialize(), &buf, &length)) {
          LOG(ERROR) << "Failed to serialize the job: " << job->get_job_name();
          return;
        }

        // lets read the configuration files
        twister2::config::ResourceConfigReader::createInstance("/home/supun/projects/twister2/twister2/config/src/yaml/conf/standalone/resource.yaml");
        twister2::config::ResourceConfigReader* reader = twister2::config::ResourceConfigReader::getInstance();
        std::string file = reader->get_twister2_job_file_location();

        // now lets write the job to file
        std::ofstream outfile(file + "/" + job->get_job_name(), std::ofstream::binary);
        outfile.write((char *)buf, length);
        outfile.close();
      }
    }
  }
}
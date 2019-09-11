#include "mpi_worker.h"
#include "glog/logging.h"
#include "resource/worker.h"
#include "resource-config-reader.h"

#include <getopt.h>
#include <stdio.h>
#include <memory>
#include <string>
#include <iostream>
#include <dlfcn.h>

std::string container_class;
std::string config_dir;
std::string twister2_home;
std::string cluster_type;
std::string job_name;
std::string job_master_ip;

int read_command_line(int argc, char* argv[]) {
  int c;
  static struct option long_options[] = {
      {"container_class", required_argument, 0, 'c'},
      {"config_dir",      required_argument, 0, 'd'},
      {"twister2_home",   required_argument, 0, 't'},
      {"cluster_type",    required_argument, 0, 'n'},
      {"job_name",        required_argument, 0, 'j'},
      {"job_master_ip",   required_argument, 0, 'i'},
      {"job_master_port", required_argument, 0, 'p'},
      {0,                 0,                 0, 0}
  };

  if (argc < 8) {
    LOG(INFO) << "Wrong arguments: " << argc;
    return -1;
  }

  while (1) {
    int option_index = 0;
    c = getopt_long(argc, argv, "c:d:t:n:j:i:p", long_options, &option_index);
    if (c == -1) {
      break;
    }
    switch (c) {
      case 'c':
        printf("option c with value '%s'\n", optarg);
        break;
      case 'd':
        printf("option d with value '%s'\n", optarg);
        break;
      case 't':
        printf("option t with value '%s'\n", optarg);
        break;
      case 'n':
        printf("option t with value '%s'\n", optarg);
        break;
      case 'j':
        printf("option t with value '%s'\n", optarg);
        break;
        case 'i':
        printf("option t with value '%s'\n", optarg);
        break;
      case 'p':
        printf("option t with value '%s'\n", optarg);
        break;
      default:
        printf("Un-expected character %d\n", c);
    }
  }
  return 0;
}

twister2::api::resource::IWorker* create_worker(std::string worker_dll) {
  void *worker_lib = dlopen(worker_dll.c_str(), RTLD_LAZY);
  if (!worker_lib) {
    LOG(ERROR) << "Cannot load library: " << dlerror() << '\n';
    return NULL;
  } else {
    LOG(INFO) << "Opening user program shared libray: " << worker_dll << std::endl;
  }
  // reset errors
  dlerror();
  // load the symbols
  twister2_iworker_create_t *create_worker = (twister2_iworker_create_t *)
      dlsym(worker_lib, "twister2_iworker_create");
  const char *dlsym_error = dlerror();
  if (dlsym_error) {
    LOG(ERROR) << "Cannot load symbol: " << dlsym_error << '\n';
    return NULL;
  }
  twister2::api::resource::IWorker* worker = create_worker();
  if (worker == NULL) {
    LOG(ERROR) << "Failed to create the worker class: " << worker_dll;
  }
  return worker;
}

int main(int argc, char* argv[]) {
  // first lets read the arguments
  read_command_line(argc, argv);

  twister2::config::ResourceConfigReader::createInstance("/home/supun/projects/twister2/twister2/config/src/yaml/conf/standalone/resource.yaml");
  twister2::config::ResourceConfigReader* reader = twister2::config::ResourceConfigReader::getInstance();
  std::cout << reader->get_twister2_job_file_location();

  // lets create the worker
  twister2::api::resource::IWorker* worker = create_worker("/home/supun/projects/twister2/bazel-bin/twister2/resource-scheduler/src/cpp/libexample_cxx.so");

  // now lets create the config
  worker->execute(NULL, 0);
  return 0;
}


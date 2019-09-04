#include "mpi_worker.h"

#include <getopt.h>
#include <stdio.h>

void read_command_line(int argc, const char* argv[]) {
  int c;
  int digit_optind = 0;

  while (1) {
    int this_option_optind = optind ? optind : 1;
    int option_index = 0;
    static struct option long_options[] = {
        {"container_class",     required_argument, 0,  0 },
        {"config_dir",  required_argument,       0,  0 },
        {"twister2_home",  required_argument, 0,  0 },
        {"cluster_type", required_argument,       0,  0 },
        {"job_name",  required_argument, 0, 0},
        {"job_master_ip",    required_argument, 0,  0 },
        {"job_master_port",         0,                 0,  0 }
    };

    c = getopt_long(argc, argv, "abc:d:012",
                    long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
      case 0:
        printf("option %s", long_options[option_index].name);
        if (optarg)
          printf(" with arg %s", optarg);
        printf("\n");
        break;

      case '0':
      case '1':
      case '2':
        if (digit_optind != 0 && digit_optind != this_option_optind)
          printf("digits occur in two different argv-elements.\n");
        digit_optind = this_option_optind;
        printf("option %c\n", c);
        break;

      case 'a':
        printf("option a\n");
        break;

      case 'b':
        printf("option b\n");
        break;

      case 'c':
        printf("option c with value '%s'\n", optarg);
        break;

      case 'd':
        printf("option d with value '%s'\n", optarg);
        break;

      case '?':
        break;

      default:
        printf("?? getopt returned character code 0%o ??\n", c);
    }
  }

  if (optind < argc) {
    LOG(INFO) << "non-option ARGV-elements: ";
    while (optind < argc) {
      LOG(INFO) << " " << argv[optind++];
    }
  }
}

int main(int argc, const char* argv[]) {
  // first lets read the arguments
  read_command_line(argc, argv);

  return 0;
}


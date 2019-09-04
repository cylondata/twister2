#ifndef TWISTER2_TWISTER2SUBMITTER_H
#define TWISTER2_TWISTER2SUBMITTER_H

#include "twister2_job.h"

class Twister2Submitter {
public:
  static void submit_job(twister2::api::Twister2Job *job);
};


#endif //TWISTER2_TWISTER2SUBMITTER_H

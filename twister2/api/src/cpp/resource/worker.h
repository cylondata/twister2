#ifndef WORKER_H_
#define WORKER_H_

##include "config.h"

namespace twister2 {
namespace resource {

class IWorker {
  public:
    virtual void execute(Config& config, int worker_id);
};

}
}

#endif


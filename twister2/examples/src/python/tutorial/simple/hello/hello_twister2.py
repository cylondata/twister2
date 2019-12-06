from twister2.Twister2Environment import Twister2Environment

env = Twister2Environment(resources=[{"cpu": 1, "ram": 512, "instances": 4}])

# Your worker code starts here
print("Hello from worker %d" % env.worker_id)

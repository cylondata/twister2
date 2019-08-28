from twister2.Twister2Context import Twister2Context

ctx = Twister2Context.init()

print("Hello from python worker %d" % ctx.worker_id)

ctx.execute(lambda x: x * x, 20)

source = ctx.lambda_source(lambda: 1, 4)

print(source)

from a import my_init, my_scheduler
import eudoxia
import sys

paramfile = "params.toml"
if len(sys.argv) > 1:
    paramfile = sys.argv[1]

eudoxia.run_simulator(paramfile)

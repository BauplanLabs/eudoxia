INIT_ALGOS = {}
SCHEDULING_ALGOS = {}


def register_scheduler_init(key):
    def decorator(func):
        if key in INIT_ALGOS: 
            raise KeyError(f"Algorithm key '{key}' is already registered.")
        INIT_ALGOS[key] = func
        return func
    return decorator

def register_scheduler(key):
    def decorator(func):
        if key in SCHEDULING_ALGOS: 
            raise KeyError(f"Algorithm key '{key}' is already registered.")
        SCHEDULING_ALGOS[key] = func
        return func
    return decorator
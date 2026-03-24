INIT_ALGOS = {}
ESTIMATOR_ALGOS = {}


def register_estimator_init(key):
    def decorator(func):
        if key in INIT_ALGOS:
            raise KeyError(f"Estimator init key '{key}' is already registered.")
        INIT_ALGOS[key] = func
        return func
    return decorator

def register_estimator(key):
    def decorator(func):
        if key in ESTIMATOR_ALGOS:
            raise KeyError(f"Estimator key '{key}' is already registered.")
        ESTIMATOR_ALGOS[key] = func
        return func
    return decorator

ESTIMATOR_ALGOS = {}


def register_estimator(key):
    def decorator(func):
        if key in ESTIMATOR_ALGOS:
            raise KeyError(f"Estimator key '{key}' is already registered.")
        ESTIMATOR_ALGOS[key] = func
        return func

    return decorator

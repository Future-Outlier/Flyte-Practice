import random

from flytekit import conditional, task, workflow

@task(cache=True, cache_serialize=True, cache_version="2.0")
def coin_toss(seed: int) -> bool:
    """
    Mimic some condition to check if the operation was successfully executed.
    """
    r = random.Random(seed)
    if r.random() < 0.5:
        return True
    return False


@task(cache=True, cache_serialize=True, cache_version="2.0")
def failed() -> int:
    """
    Mimic a task that handles failure
    """
    return -1


@task(cache=True, cache_serialize=True, cache_version="2.0")
def success() -> int:
    """
    Mimic a task that handles success
    """
    return 0


@workflow
def basic_boolean_wf(seed: int = 5) -> int:
    result = coin_toss(seed=seed)
    return conditional("test").if_(result.is_true()).then(success()).else_().then(failed())

@workflow
def bool_input_wf(b: bool) -> int:
    return conditional("test").if_(b.is_true()).then(success()).else_().then(failed())

if __name__ == "__main__":
    print("Running basic_boolean_wf a few times")
    for i in range(0, 5):
        print(f"Basic boolean wf output {basic_boolean_wf()}")
        print(f"Boolean input {True if i < 2 else False}, workflow output {bool_input_wf(b=True if i < 2 else False)}")
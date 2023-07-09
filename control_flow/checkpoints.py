from flytekit import current_context, task, workflow
from flytekit.exceptions.user import FlyteRecoverableException

RETRIES = 3

@task(retries=RETRIES)
def use_checkpoint(n_iterations: int) -> int:
    cp = current_context().checkpoint
    prev = cp.read()
    start = 0
    if prev:
        start = int(prev.decode())

    # create a failure interval so we can create failures for across 'n' iterations and then succeed after
    # configured retries
    failure_interval = n_iterations // RETRIES
    i = 0
    for i in range(start, n_iterations):
        # simulate a deterministic failure, for demonstration. We want to show how it eventually completes within
        # the given retries
        if i > start and i % failure_interval == 0:
            raise FlyteRecoverableException(f"Failed at iteration {i}, failure_interval {failure_interval}")
        # save progress state. It is also entirely possible save state every few intervals.
        cp.write(f"{i + 1}".encode())

    return i

@workflow
def example(n_iterations: int) -> int:
    return use_checkpoint(n_iterations=n_iterations)

if __name__ == "__main__":
    try:
        example(n_iterations=10)
    except RuntimeError as e:  # noqa : F841
        # no retries are performed, so an exception is expected when run locally.
        pass
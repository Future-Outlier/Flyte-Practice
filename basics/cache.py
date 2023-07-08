import time

import pandas
from flytekit import HashMethod, task, workflow
from flytekit.core.node_creation import create_node
from typing_extensions import Annotated

@task(cache=True, cache_serialize=True, cache_version="2.0")  # noqa: F841
def square2(a: int, b: int) -> int:
    """
     Parameters:
        n (int): name of the parameter for the task will be derived from the name of the input variable.
                 The type will be automatically deduced to ``Types.Integer``.

    Return:
        int: The label for the output will be automatically assigned, and the type will be deduced from the annotation.

    """
    print("sleep for a whlie")
    time.sleep(2)
    return a * a + b * b

@workflow
def wf(a: int, b: int) -> int:
    return square2(a=a, b=b)


if __name__ == "__main__":
    # print(f"result {wf(a=1, b=2)}")
    print(f"result {wf(a=1, b=2)}")


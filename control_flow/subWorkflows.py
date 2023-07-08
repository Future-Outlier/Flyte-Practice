import typing
from typing import Tuple

from flytekit import task, workflow

op = typing.NamedTuple("OutputsBC", t1_int_output=int, c=str)

@task
def t1(a: int) -> op:
    return op(a + 2, "world")

@workflow
def my_subwf(a: int = 42) -> Tuple[str, str]:
    x, y = t1(a=a)
    u, v = t1(a=x)
    return y, v

@workflow
def parent_wf(a: int) -> Tuple[int, str, str]:
    x, y = t1(a=a).with_overrides(node_name="node-t1-parent") # update the graph node
    u, v = my_subwf(a=x)
    return x, u, v

# if __name__ == "__main__":
#     print(f"Running parent_wf(a=3) {parent_wf(a=3)}")


# Interestingly, we can nest a workflow that has a subworkflow within a workflow.
# Workflows can be simply composed from other workflows, even if they are standalone entities. Each of the
# workflows in this module can exist and run independently.
@workflow
def nested_parent_wf(a: int) -> Tuple[int, str, str, str]:
    x, y = my_subwf(a=a)
    m, n, o = parent_wf(a=a)
    return m, n, o, y

if __name__ == "__main__":
    print(f"Running nested_parent_wf(a=3) {nested_parent_wf(a=3)}")
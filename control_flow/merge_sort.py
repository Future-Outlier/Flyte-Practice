import typing
from datetime import datetime
from random import random, seed
from typing import Tuple

from flytekit import conditional, dynamic, task, workflow

# seed random number generator
seed(datetime.now().microsecond)

@task
def split(numbers: typing.List[int]) -> Tuple[typing.List[int], typing.List[int], int]:
    print("split1:", numbers[0 : int(len(numbers) / 2)])
    print("split2:", numbers[int(len(numbers) / 2) :])
    print("Number Count:", int(len(numbers) / 2))

    return (
        numbers[0 : int(len(numbers) / 2)],
        numbers[int(len(numbers) / 2) :],
        int(len(numbers) / 2),
    )

@task
def merge(sorted_list1: typing.List[int], sorted_list2: typing.List[int]) -> typing.List[int]:
    result = []
    while len(sorted_list1) > 0 and len(sorted_list2) > 0:
        # Check if current element of first array is smaller than current element of second array. If yes,
        # store first array element and increment first array index. Otherwise do same with second array
        if sorted_list1[0] < sorted_list2[0]:
            result.append(sorted_list1.pop(0))
        else:
            result.append(sorted_list2.pop(0))

    result.extend(sorted_list1)
    result.extend(sorted_list2)

    return result

@task
def sort_locally(numbers: typing.List[int]) -> typing.List[int]:
    print("sorted length:", len(numbers))
    print("sorted numbers:", sorted(numbers))
    return sorted(numbers)

@dynamic
def merge_sort_remotely(numbers: typing.List[int], run_local_at_count: int) -> typing.List[int]:
    split1, split2, new_count = split(numbers=numbers)
    # print(f"split1:{split1}, split2:{split2}, new_count:{new_count}")
    sorted1 = merge_sort(numbers=split1, numbers_count=new_count, run_local_at_count=run_local_at_count)
    sorted2 = merge_sort(numbers=split2, numbers_count=len(numbers)-new_count, run_local_at_count=run_local_at_count)
    return merge(sorted_list1=sorted1, sorted_list2=sorted2)

@workflow
def merge_sort(numbers: typing.List[int], numbers_count: int, run_local_at_count: int = 10) -> typing.List[int]:
    return (
        conditional("terminal_case")
        .if_(numbers_count <= run_local_at_count)
        .then(sort_locally(numbers=numbers))
        .else_()
        .then(merge_sort_remotely(numbers=numbers, run_local_at_count=run_local_at_count))
    )

def generate_inputs(numbers_count: int) -> typing.List[int]:
    generated_list = []
    # generate random numbers between 0-1
    for _ in range(numbers_count):
        value = int(random() * 10000)
        generated_list.append(value)

    return generated_list

if __name__ == "__main__":
    count = 25
    x = generate_inputs(count)
    print(x)
    print(f"Running Merge Sort Locally...{merge_sort(numbers=x, numbers_count=count)}")

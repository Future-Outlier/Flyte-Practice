import typing  # noqa: E402

from flytekit import Resources, task, workflow  # noqa: E402

@task(requests=Resources(cpu="2", mem="200Mi"), limits=Resources(cpu="3", mem="350Mi"))
def count_unique_numbers_1(x: typing.List[int]) -> int:
    s = set()
    for i in x:
        s.add(i)
    return len(s)

@task
def square_1(x: int) -> int:
    return x * x

@workflow
def my_pipeline(x: typing.List[int]) -> int:
    return square_1(x=count_unique_numbers_1(x=x)).with_overrides(limits=Resources(cpu="6", mem="500Mi"))

if __name__ == "__main__":
    print(count_unique_numbers_1(x=[1, 1, 2]))
    print(my_pipeline(x=[1, 1, 2]))

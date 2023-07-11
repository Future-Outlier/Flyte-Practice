from flytekit import task, workflow
from typing import List


from flytekit.types.pickle.pickle import BatchSize
from typing_extensions import Annotated


class People:
    def __init__(self, name):
        self.name = name

@task
def greet(name: str) -> People:
    return People(name)


@workflow
def welcome(name: str) -> People:
    return greet(name=name)


# if __name__ == "__main__":
#     """
#     This workflow can be run locally. During local execution also,
#     the custom object (People) will be marshalled to and from python pickle.
#     """
#     welcome(name="Foo")





@task
def greet_all(names: List[str]) -> Annotated[List[People], BatchSize(2)]:
    return [People(name) for name in names]


@workflow
def welcome_all(names: List[str]) -> Annotated[List[People], BatchSize(2)]:
    return greet_all(names=names)


if __name__ == "__main__":
    """
    In this example, two pickle files will be generated:
    - One containing two People objects
    - One containing one People object
    """
    print(welcome_all(names=["f", "o", "o"]))
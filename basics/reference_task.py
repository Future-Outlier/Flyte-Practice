from flytekit import task

@task
def add(a: int, b: int) -> int:
    return a + b

serialized_task = add.serialize()

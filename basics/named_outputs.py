import typing

from flytekit import task, workflow

hello_output = typing.NamedTuple("OP", greet=str)

@task
def say_hello() -> hello_output:
    return hello_output("hello world")

wf_outputs = typing.NamedTuple("OP2", greet1=str, greet2=str)

@workflow
def my_wf() -> wf_outputs:
    return  (say_hello().greet, say_hello().greet)

if __name__ == "__main__":
    print(f"Running my_wf() {my_wf()}")

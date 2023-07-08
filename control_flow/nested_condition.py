import random

from flytekit import conditional, task, workflow

@task
def square(n: float) -> float:
    """
    Parameters:
        n (float): name of the parameter for the task is derived from the name of the input variable, and
               the type is automatically mapped to Types.Integer
    Return:
        float: The label for the output is automatically assigned and the type is deduced from the annotation
    """
    return n * n


@task
def double(n: float) -> float:
    """
    Parameters:
        n (float): name of the parameter for the task is derived from the name of the input variable
               and the type is mapped to ``Types.Integer``
    Return:
        float: The label for the output is auto-assigned and the type is deduced from the annotation
    """
    return 2 * n

@workflow
def nested_conditions(my_input: float) -> float:
    return (
        conditional("fractions")
        .if_((my_input > 0.1) & (my_input < 1.0))
        .then(
            conditional("inner_fractions")
            .if_(my_input < 0.5)
            .then(double(n=my_input))
            .elif_((my_input > 0.5) & (my_input < 0.7))
            .then(square(n=my_input))
            .else_()
            .fail("Only <0.7 allowed")
        )
        .elif_((my_input > 1.0) & (my_input < 10.0))
        .then(square(n=my_input))
        .else_()
        .then(double(n=my_input))
    )

if __name__ == "__main__":
    print(f"nested_conditions(0.6) -> {nested_conditions(my_input=0.6)}")


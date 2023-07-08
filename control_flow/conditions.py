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


# @workflow
# def multiplier(my_input: float) -> float:
#     return (
#         conditional("fractions")
#         .if_((my_input >= 0.1) & (my_input <= 1.0))
#         .then(double(n=my_input))
#         .else_()
#         .then(square(n=my_input))
#     )


# if __name__ == "__main__":
#     print(f"Output of multiplier(my_input=3.0): {multiplier(my_input=3.0)}")
#     print(f"Output of multiplier(my_input=0.5): {multiplier(my_input=0.5)}")

# @workflow
# def multiplier_2(my_input: float) -> float:
#     return (
#         conditional("fractions")
#         .if_((my_input > 0.1) & (my_input < 1.0))
#         .then(double(n=my_input))
#         .elif_((my_input > 1.0) & (my_input <= 10.0))
#         .then(square(n=my_input))
#         .else_()
#         .fail("The input must be between 0 and 10")
#     )


# if __name__ == "__main__":
#     print(f"Output of multiplier_2(my_input=10.0): {multiplier_2(my_input=10.0)}")

@workflow
def multiplier_3(my_input: float) -> float:
    result = (
        conditional("fractions")
        .if_((my_input > 0.1) & (my_input < 1.0))
        .then(double(n=my_input))
        .elif_((my_input > 1.0) & (my_input < 10.0))
        .then(square(n=my_input))
        .else_()
        .fail("The input must be between 0 and 10")
    )

    # the 'result' will either be the output of `double` or `square`. If none of the conditions is true,
    # it gives a failure message.
    return double(n=result)


if __name__ == "__main__":
    print(f"Output of multiplier_3(my_input=5.0): {multiplier_3(my_input=4.0)}")
from math import sqrt
from typing import List
from flytekit import task, workflow
from time import sleep

@task
def mean(values: List[float]) -> float:
    return sum(values) / len(values)
    
@task
def standard_deviation(values: List[float], mu: float) -> float:
    variance = sum([(x - mu) ** 2 for x in values])
    return sqrt(variance)

@task
def standard_scale(values: List[float], mu: float, sigma: float) -> List[float]:
    return [(x - mu) / sigma for x in values]

@workflow
def standard_scale_workflow(values: List[float]) -> List[float]:
    mu = mean(values=values)
    print(mu)  # this is not the actual float value!
    sigma = standard_deviation(values=values, mu=mu)
    return standard_scale(values=values, mu=mu, sigma=sigma)

import random

@task
def generate_data(num_samples: int, seed: int) -> List[float]:
    random.seed(seed)
    return [random.random() for _ in range(num_samples)]

@workflow
def workflow_with_subworkflow(num_samples: int, seed: int) -> List[float]:
    data = generate_data(num_samples=num_samples, seed=seed)
    return standard_scale_workflow(values=data)

# print(workflow_with_subworkflow(num_samples=10, seed=3))

# sleep(3)
# print(standard_scale_workflow(values=[float(i) for i in range(1, 11)]))

# @task
# def buggy_standard_scale(values: List[float], mu: float, sigma: float) -> float:
#     """
#     🐞 The implementation and output type of this task is incorrect! It should
#     be List[float] instead of a sum of all the scaled values.
#     """
#     return sum([(x - mu) / sigma for x in values])

# @workflow
# def buggy_standard_scale_workflow(values: List[float]) -> List[float]:
#     mu = mean(values=values)
#     sigma = standard_deviation(values=values, mu=mu)
#     return buggy_standard_scale(values=values, mu=mu, sigma=sigma)

# try:
#     buggy_standard_scale_workflow(values=[float(i) for i in range(1, 11)])
# except Exception as e:
#     print(e)

from flytekit import LaunchPlan

standard_scale_launch_plan = LaunchPlan.get_or_create(
    standard_scale_workflow,
    name="standard_scale_lp",
    default_inputs={"values": [3.0, 4.0, 5.0]}
)

# default
print(standard_scale_launch_plan())

# overriden
print(standard_scale_launch_plan(values=[float(x) for x in range(20, 30)]))

@workflow
def workflow_with_launchplan(num_samples: int, seed: int) -> List[float]:
    data = generate_data(num_samples=num_samples, seed=seed)
    return standard_scale_launch_plan(values=data)

print(workflow_with_launchplan(num_samples=10, seed=3))
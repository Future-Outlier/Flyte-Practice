import typing  # noqa: E402
from collections import Counter  # noqa: E402
from typing import Dict, Tuple  # noqa: E402

from flytekit import LaunchPlan, task, workflow  # noqa: E402

@task
def count_freq_words(input_string1: str) -> Dict:
    # input_string = "The cat sat on the mat"
    words = input_string1.split()
    wordCount = dict(Counter(words))
    return wordCount

@workflow
def ext_workflow(my_input: str) -> Dict:
    result = count_freq_words(input_string1=my_input)
    return result

external_lp = LaunchPlan.get_or_create(
    ext_workflow,
    "parent_workflow_execution",
)

@task
def count_repetitive_words(word_counter: Dict) -> typing.List[str]:
    repeated_words = [key for key, value in word_counter.items() if value > 1]
    return repeated_words

@workflow
def parent_workflow(my_input1: str) -> typing.List[str]:
    my_op1 = external_lp(my_input=my_input1)
    my_op2 = count_repetitive_words(word_counter=my_op1)
    return my_op2

if __name__ == "__main__":
    print("Running parent workflow...")
    print(parent_workflow(my_input1="the cat took the apple and ate the apple"))

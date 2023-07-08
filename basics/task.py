from flytekit import task
# Importing additional modules.
from sklearn.datasets import load_iris
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

@task
def train_model(hyperparameters: dict, test_size: float, random_state: int) -> LogisticRegression:
    """
    Parameters:
        hyperparameters (dict): A dictionary containing the hyperparameters for the model.
        test_size (float): The proportion of the data to be used for testing.
        random_state (int): The random seed for reproducibility.

    Return:
        LogisticRegression: The trained logistic regression model.
    """
    # Loading the Iris dataset
    iris = load_iris()

    # Splitting the data into train and test sets
    X_train, _, y_train, _ = train_test_split(iris.data, iris.target, test_size=test_size, random_state=random_state)

    # Creating and training the logistic regression model with the given hyperparameters
    clf = LogisticRegression(**hyperparameters)
    clf.fit(X_train, y_train)

    return clf

from flytekit import workflow


@workflow
def train_model_wf(
    hyperparameters: dict = {"C": 0.1}, test_size: float = 0.2, random_state: int = 42
) -> LogisticRegression:
    """
    This workflow invokes the train_model task with the given hyperparameters, test size and random state.
    """
    return train_model(hyperparameters=hyperparameters, test_size=test_size, random_state=random_state)

import functools


@workflow
def train_model_wf_with_partial(test_size: float = 0.2, random_state: int = 42) -> LogisticRegression:
    partial_task = functools.partial(train_model, hyperparameters={"C": 0.1})
    return partial_task(test_size=test_size, random_state=random_state)


if __name__ == "__main__":
    print(train_model(hyperparameters={"C": 0.1}, test_size=0.2, random_state=42))
    print(train_model_wf_with_partial())
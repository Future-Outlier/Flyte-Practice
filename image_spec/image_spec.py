import typing

import pandas as pd
from flytekit import ImageSpec, Resources, task, workflow

pandas_image_spec = ImageSpec(
    base_image="ghcr.io/flyteorg/flytekit:py3.8-1.6.2",
    packages=["pandas", "numpy"],
    python_version="3.9",
    apt_packages=["git"],
    env={"Debug": "True"},
    registry="ghcr.io/unionai-oss",
)

sklearn_image_spec = ImageSpec(
    base_image="ghcr.io/flyteorg/flytekit:py3.8-1.6.2",
    packages=["scikit-learn"],
    registry="ghcr.io/unionai-oss",
)

if sklearn_image_spec.is_container():
    from sklearn.linear_model import LogisticRegression

@task(container_image=pandas_image_spec)
def get_pandas_dataframe() -> typing.Tuple[pd.DataFrame, pd.Series]:
    df = pd.read_csv("https://storage.googleapis.com/download.tensorflow.org/data/heart.csv")
    print(df.head())
    return df[["age", "thalach", "trestbps", "chol", "oldpeak"]], df.pop("target")


@task(container_image=sklearn_image_spec, requests=Resources(cpu="1", mem="1Gi"))
def get_model(max_iter: int, multi_class: str) -> typing.Any:
    import platform

    print(platform.python_version())

    return LogisticRegression(max_iter=max_iter, multi_class=multi_class)


# Get a basic model to train.
@task(container_image=sklearn_image_spec, requests=Resources(cpu="1", mem="1Gi"))
def train_model(model: typing.Any, feature: pd.DataFrame, target: pd.Series) -> typing.Any:
    import platform

    print(platform.python_version())

    model.fit(feature, target)
    return model


# Lastly, let's define a workflow to capture the dependencies between the tasks.
@workflow()
def wf():
    feature, target = get_pandas_dataframe()
    model = get_model(max_iter=3000, multi_class="auto")
    train_model(model=model, feature=feature, target=target)


if __name__ == "__main__":
    wf()

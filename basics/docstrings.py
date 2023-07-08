from dataclasses import dataclass

import pandas as pd
from dataclasses_json import dataclass_json
from flytekit import task, workflow

@dataclass_json
@dataclass
class PandasData(object):
    id: int = 3
    name: str = "Bonnie"

@task
def add_data(df: pd.DataFrame, data: PandasData) -> pd.DataFrame:
    df = df.append({"id": data.id, "name": data.name}, ignore_index=True)
    return df

@workflow
def sphinx_docstring(df: pd.DataFrame, data: PandasData = PandasData()) -> pd.DataFrame:
    """
    Showcase Sphinx-style docstring.

    This workflow accepts a DataFrame and data class.
    It calls a task that appends the user-sent record to the DataFrame.

    :param df: Pandas DataFrame
    :param data: A data class pertaining to the new record to be stored in the DataFrame
    :return: Pandas DataFrame
    """
    return add_data(df=df, data=data)

@workflow
def numpy_docstring(df: pd.DataFrame, data: PandasData = PandasData()) -> pd.DataFrame:
    """
    Showcase NumPy-style docstring.

    This workflow accepts a DataFrame and data class.
    It calls a task that appends the user-sent record to the DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        Pandas DataFrame
    data: Dataclass
        A data class pertaining to the new record to be stored in the DataFrame

    Returns
    -------
    out : pd.DataFrame
        Pandas DataFrame
    """
    return add_data(df=df, data=data)

@workflow
def google_docstring(df: pd.DataFrame, data: PandasData = PandasData()) -> pd.DataFrame:
    """
    Showcase Google-style docstring.

    This workflow accepts a DataFrame and data class.
    It calls a task that appends the user-sent record to the DataFrame.

    Args:
        df(pd.DataFrame): Pandas DataFrame
        data(Dataclass): A data class pertaining to the new record to be stored in the DataFrame
    Returns:
        pd.DataFrame: Pandas DataFrame
    """
    return add_data(df=df, data=data)

if __name__ == "__main__":
    print(f"Running {__file__} main...")
    print(
        f"Running sphinx_docstring(), modified DataFrame is {sphinx_docstring(df=pd.DataFrame(data={'id': [1, 2], 'name': ['John', 'Meghan']}),data=PandasData(id=3, name='Bonnie'))}"
    )
    print(
        f"Running numpy_docstring(), modified DataFrame is {numpy_docstring(df=pd.DataFrame(data={'id': [1, 2], 'name': ['John', 'Meghan']}),data=PandasData(id=3, name='Bonnie'))}"
    )
    print(
        f"Running google_docstring(), modified DataFrame is {google_docstring(df=pd.DataFrame(data={'id': [1, 2], 'name': ['John', 'Meghan']}),data=PandasData(id=3, name='Bonnie'))}"
    )

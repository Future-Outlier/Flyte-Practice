import os
import typing

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from flytekit import FlyteContext, StructuredDatasetType, kwtypes, task, workflow
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.types.schema import FlyteSchema
from flytekit.types.structured.structured_dataset import (
    PARQUET,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
    StructuredDatasetTransformerEngine,
)
from typing_extensions import Annotated

import pandas as pd  # noqa: E402
from flytekit import task  # noqa: E402
from flytekit.types.structured import StructuredDataset  # noqa: E402

superset_cols = kwtypes(Name=str, Age=int, Height=int)
subset_cols = kwtypes(Age=int)



@task
def get_df(a: int) -> Annotated[pd.DataFrame, superset_cols]:
    """
    Generate a sample dataframe
    """
    return pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [a, 22], "Height": [160, 178]})


@task
def get_schema_df(a: int) -> FlyteSchema[superset_cols]:
    """
    Generate a sample dataframe
    """
    s = FlyteSchema()
    s.open().write(pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [a, 22], "Height": [160, 178]}))
    return s

@task
def get_subset_df(df: Annotated[StructuredDataset, subset_cols]) -> Annotated[StructuredDataset, subset_cols]:
    df = df.open(pd.DataFrame).all()
    df = pd.concat([df, pd.DataFrame([[30]], columns=["Age"])])
    # On specifying BigQuery uri for StructuredDataset, flytekit writes a pandas dataframe to a BigQuery table
    return StructuredDataset(dataframe=df)
class NumpyEncodingHandlers(StructuredDatasetEncoder):
    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        df = typing.cast(np.ndarray, structured_dataset.dataframe)
        name = ["col" + str(i) for i in range(len(df))]
        table = pa.Table.from_arrays(df, name)
        path = ctx.file_access.get_random_remote_directory()
        local_dir = ctx.file_access.get_random_local_directory()
        local_path = os.path.join(local_dir, f"{0:05}")
        pq.write_table(table, local_path)
        ctx.file_access.upload_directory(local_dir, path)
        return literals.StructuredDataset(
            uri=path,
            metadata=StructuredDatasetMetadata(structured_dataset_type=StructuredDatasetType(format=PARQUET)),
        )
class NumpyDecodingHandlers(StructuredDatasetDecoder):
    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> np.ndarray:
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.get_data(flyte_value.uri, local_dir, is_multipart=True)
        table = pq.read_table(local_dir)
        return table.to_pandas().to_numpy()
class NumpyRenderer:
    """
    The schema of Numpy array are rendered as an HTML table.
    """

    def to_html(self, df: np.ndarray) -> str:
        assert isinstance(df, np.ndarray)
        name = ["col" + str(i) for i in range(len(df))]
        table = pa.Table.from_arrays(df, name)
        return pd.DataFrame(table.schema).to_html(index=False)

StructuredDatasetTransformerEngine.register(NumpyEncodingHandlers(np.ndarray, None, PARQUET))
StructuredDatasetTransformerEngine.register(NumpyDecodingHandlers(np.ndarray, None, PARQUET))
StructuredDatasetTransformerEngine.register_renderer(np.ndarray, NumpyRenderer())

@task
def to_numpy(ds: Annotated[StructuredDataset, subset_cols]) -> Annotated[StructuredDataset, subset_cols, PARQUET]:
    numpy_array = ds.open(np.ndarray).all()
    return StructuredDataset(dataframe=numpy_array)

@workflow
def pandas_compatibility_wf(a: int) -> Annotated[StructuredDataset, subset_cols]:
    df = get_df(a=a)
    ds = get_subset_df(df=df)  # noqa: shown for demonstration; users should use the same types between tasks
    return to_numpy(ds=ds)


@workflow
def schema_compatibility_wf(a: int) -> Annotated[StructuredDataset, subset_cols]:
    df = get_schema_df(a=a)
    ds = get_subset_df(df=df)  # noqa: shown for demonstration; users should use the same types between tasks
    return to_numpy(ds=ds)

if __name__ == "__main__":
    numpy_array_one = pandas_compatibility_wf(a=42).open(np.ndarray).all()
    print(f"pandas DataFrame compatibility check output: {numpy_array_one}")
    numpy_array_two = schema_compatibility_wf(a=42).open(np.ndarray).all()
    print(f"Schema compatibility check output: {numpy_array_two}")
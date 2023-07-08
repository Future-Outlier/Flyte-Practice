import logging
from io import StringIO

import pandas as pd
from botocore import session
from flytekit import task, workflow
from flytekit.configuration import S3Config

logger = logging.getLogger(__file__)

CSV_FILE = "iris.csv"
BUCKET_NAME = "chain-flyte-entities"


def s3_client():
    cfg = S3Config.auto()
    sess = session.get_session()
    return sess.create_client(
        "s3",
        aws_access_key_id=cfg.access_key_id,
        aws_secret_access_key=cfg.secret_access_key,
        use_ssl=False,
        endpoint_url=cfg.endpoint,
    )

@task(cache=True, cache_version="1.0")
def create_bucket():
    client = s3_client()
    try:
        client.create_bucket(Bucket=BUCKET_NAME)
    except client.exceptions.BucketAlreadyOwnedByYou:
        logger.info(f"Bucket {BUCKET_NAME} has already been created by you.")

@task
def read() -> pd.DataFrame:
    data = pd.read_csv(s3_client().get_object(Bucket=BUCKET_NAME, Key=CSV_FILE)["Body"])
    return data

@task(cache=True, cache_version="1.0")
def write():
    df = pd.DataFrame(  # noqa : F841
        data={
            "sepal_length": [5.3],
            "sepal_width": [3.8],
            "petal_length": [0.1],
            "petal_width": [0.3],
            "species": ["setosa"],
        }
    )
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_client().put_object(Body=csv_buffer.getvalue(), Bucket=BUCKET_NAME, Key=CSV_FILE)

@workflow
def chain_tasks_wf() -> pd.DataFrame:
    create_bucket_promise = create_bucket()
    write_promise = write()
    read_promise = read()

    create_bucket_promise >> write_promise
    write_promise >> read_promise

    return read_promise

@workflow
def write_sub_workflow():
    write()


@workflow
def read_sub_workflow() -> pd.DataFrame:
    return read()

@workflow
def chain_workflows_wf() -> pd.DataFrame:
    create_bucket_promise = create_bucket()
    write_sub_wf = write_sub_workflow()
    read_sub_wf = read_sub_workflow()

    create_bucket_promise >> write_sub_wf
    write_sub_wf >> read_sub_wf

    return read_sub_wf

if __name__ == "__main__":
    print(f"Running {__file__} main...")
    print(f"Running chain_tasks_wf()... {chain_tasks_wf()}")
    print(f"Running chain_workflows_wf()... {chain_workflows_wf()}")

import datetime

import pandas
from flytekit import SQLTask, TaskMetadata, kwtypes, task, workflow
from flytekit.testing import patch, task_mock
from flytekit.types.schema import FlyteSchema

sql = SQLTask(
    "my-query",
    query_template="SELECT * FROM hive.city.fact_airport_sessions WHERE ds = '{{ .Inputs.ds }}' LIMIT 10",
    inputs=kwtypes(ds=datetime.datetime),
    outputs=kwtypes(results=FlyteSchema),
    metadata=TaskMetadata(retries=2),
)

@task
def t1() -> datetime.datetime:
    return datetime.datetime.now()

@workflow
def my_wf() -> FlyteSchema:
    dt = t1()
    return sql(ds=dt)

def main_1():
    with task_mock(sql) as mock:
        mock.return_value = pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})
        assert (my_wf().open().all() == pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})).all().all()

def main_2():
    @patch(sql)
    def test_user_demo_test(mock_sql):
        mock_sql.return_value = pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})
        assert (my_wf().open().all() == pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})).all().all()

    test_user_demo_test()


if __name__ == "__main__":
    main_1()
    main_2()


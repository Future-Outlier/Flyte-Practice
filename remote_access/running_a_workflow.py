from flytekit.remote import FlyteRemote
from flytekit.configuration import Config

# FlyteRemote object is the main entrypoint to API
remote = FlyteRemote(
    config=Config.for_endpoint(endpoint="flyte.example.net"),
    default_project="flytesnacks",
    default_domain="development",
)

# Fetch workflow
flyte_workflow = remote.fetch_workflow(name="workflows.example.wf", version="v1")

# Execute
execution = remote.execute(
    flyte_workflow, inputs={"mean": 1}, execution_name="workflow_execution", wait=True
)

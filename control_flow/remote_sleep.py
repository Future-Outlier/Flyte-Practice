import typing
from flytekit.remote.remote import FlyteRemote
from flytekit.configuration import Config

remote = FlyteRemote(
    Config.for_sandbox(),
    default_project="flytesnacks",
    default_domain="development",
)

# First kick off the wotrkflow
flyte_workflow = remote.fetch_workflow(
    name="core.control_flow.waiting_for_external_inputs.conditional_wf"
)

# Execute the workflow
execution = remote.execute(flyte_workflow, inputs={"data": [1.0, 2.0, 3.0, 4.0, 5.0]})

# Get a list of signals available for the execution
signals = remote.list_signals(execution.id.name)

# Set a signal value for the "title" node. Make sure that the "title-input"
# node is in the `signals` list above
remote.set_signal("title-input", execution.id.name, "my report")

# Set signal value for the "review-passes" node. Make sure that the "review-passes"
# node is in the `signals` list above
remote.set_signal("review-passes", execution.id.name, True)
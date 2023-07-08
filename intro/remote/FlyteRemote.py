from flytekit.remote import FlyteRemote
from flytekit.configuration import Config

remote = FlyteRemote(config=Config.auto())
flyte_wf = remote.fetch_workflow(name="workflows.example.wf")
execution = remote.execute(flyte_wf, inputs={"name": "Kermit"})
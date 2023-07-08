import flytekit
import pandas as pd
import plotly.express as px
from flytekit import task, workflow
from flytekit.deck.renderer import TopFrameRenderer
from flytekitplugins.deck.renderer import BoxRenderer, MarkdownRenderer
from typing_extensions import Annotated

# Fetch iris data.  
iris_df = px.data.iris()

@task(disable_deck=False)
def t1() -> str:
    md_text = "#Hello Flyte\n##Hello Flyte\n###Hello Flyte"
    flytekit.Deck("demo", BoxRenderer("sepal_length").to_html(iris_df))
    flytekit.current_context().default_deck.append(MarkdownRenderer().to_html(md_text))
    return md_text

@task(disable_deck=False)
def t2() -> Annotated[pd.DataFrame, TopFrameRenderer(10)]:
    return iris_df

@workflow
def wf():
    t1()
    t2()

if __name__ == "__main__":
    wf()
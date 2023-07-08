import pandas as pd
import plotly.express as px
from flytekit import task, workflow
from typing import Optional
from flytekit import task, workflow


@task(disable_deck=False)
def iris_data(
    sample_frac: Optional[float] = None,
    random_state: Optional[int] = None,
) -> pd.DataFrame:
    data = px.data.iris()
    if sample_frac is not None:
        data = data.sample(frac=sample_frac, random_state=random_state)
    return data


@workflow
def wf(
    sample_frac: Optional[float] = None,
    random_state: Optional[int] = None,
):
    iris_data(sample_frac=sample_frac, random_state=random_state)

import flytekit
from flytekitplugins.deck.renderer import MarkdownRenderer, BoxRenderer

@task(disable_deck=False)
def iris_data(
    sample_frac: Optional[float] = None,
    random_state: Optional[int] = None,
) -> pd.DataFrame:
    data = px.data.iris()
    if sample_frac is not None:
        data = data.sample(frac=sample_frac, random_state=random_state)

    md_text = (
        "# Iris Dataset\n"
        "This task loads the iris dataset using the  `plotly` package."
    )
    flytekit.current_context().default_deck.append(MarkdownRenderer().to_html(md_text))
    flytekit.Deck("box plot", BoxRenderer("sepal_length").to_html(data))
    return data

print(wf(sample_frac=1.0, random_state=42))

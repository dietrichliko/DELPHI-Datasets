"""DELPHI Datasets"""

import click

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import delphi_datasets.model as model


@click.command
def main() -> None:
    engine = create_engine("sqlite:///test.db", echo=True)
    model.Base.metadata.create_all(engine)

    with Session(engine) as session:
        dataset1 = model.Dataset(
            name="parent",
            description="",
            recid=1,
            version="95c1",
            channel="",
            format="SHORT",
            data=True,
            status=model.Status.OK,
        )
        session.add(dataset1)
        dataset2 = model.Dataset(
            name="child",
            description="",
            recid=1,
            version="95c1",
            channel="",
            format="SHORT",
            data=True,
            status=model.Status.OK,
        )
        session.add(dataset2)
        dataset1.children.add(dataset2)
        session.commit()

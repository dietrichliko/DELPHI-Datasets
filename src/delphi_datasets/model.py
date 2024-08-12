"""DELPHI Datasets DB Model"""

import enum
import os
import pathlib
from typing import Optional, Set

from sqlalchemy import Boolean, Column, ForeignKey, String, Table, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship


class Status(enum.Enum):
    OK = 0
    INCOMPLETE = 1
    EMPTY = 2


class Base(DeclarativeBase):
    pass


association_table_1 = Table(
    "association_table_1",
    Base.metadata,
    Column("dataset_id", ForeignKey("datasets.id"), primary_key=True),
    Column("file_id", ForeignKey("files.id"), primary_key=True),
)

association_table_2 = Table(
    "association_table_2",
    Base.metadata,
    Column("dataset_id", ForeignKey("datasets.id"), primary_key=True),
    Column("year_id", ForeignKey("years.id"), primary_key=True),
)


class Dataset(Base):
    __tablename__ = "datasets"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30), index=True, unique=True)
    recid: Mapped[int]
    version: Mapped[str] = mapped_column(String(5))
    channel: Mapped[str] = mapped_column(String(10))
    format: Mapped[str] = mapped_column(String(10))
    status: Mapped[Status]
    data: Mapped[bool] = mapped_column(Boolean)
    files: Mapped[Set["File"]] = relationship(
        secondary=association_table_1, back_populates="datasets"
    )
    years: Mapped[Set["Year"]] = relationship(
        secondary=association_table_2, back_populates="datasets"
    )

    def __repr__(self) -> str:
        return f"Dataset(id={self.id}, name='{self.name}', recid={self.recid}, version='{self.version}')"


class File(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(primary_key=True)
    path: Mapped[str] = mapped_column(index=True, unique=True)
    size: Mapped[int]
    checksum: Mapped[int]
    entries: Mapped[int] = mapped_column(nullable=True)
    events: Mapped[int] = mapped_column(nullable=True)
    datasets: Mapped[Set[Dataset]] = relationship(
        secondary=association_table_1, back_populates="files"
    )

    def __repr__(self) -> str:
        return f"File(id={self.id}, path='{self.path}', size={self.size})"

    @staticmethod
    def get_instance(session: Session, path: pathlib.Path) -> Optional["File"]:
        result = session.execute(select(File).where(File.path == str(path))).first()
        if result is not None:
            return result[0]

        try:
            size = path.stat().st_size
        except FileNotFoundError:
            return
        checksum = int(os.getxattr(path, "eos.checksum").decode("utf-8"), 16)
        file_obj = File(
            path=str(path),
            size=size,
            checksum=checksum,
        )
        session.add(file_obj)
        return file_obj


class Year(Base):
    __tablename__ = "years"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(4), index=True, unique=True)
    datasets: Mapped[Set[Dataset]] = relationship(
        secondary=association_table_2, back_populates="years"
    )

    @staticmethod
    def get_instance(session: Session, year: str) -> "Year":
        result = session.execute(select(Year).where(Year.name == year)).first()
        if result is not None:
            return result[0]

        year_obj = Year(name=year)
        session.add(year_obj)
        return year_obj

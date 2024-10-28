"""DELPHI Datasets DB Model."""

from __future__ import annotations

import enum
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pathlib

from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Table, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship


class Status(enum.Enum):
    """Dataset status."""

    OK = 0
    INCOMPLETE = 1
    EMPTY = 2


class Base(DeclarativeBase):
    """SQLAlchemy Base."""


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

association_table_3 = Table(
    "association_table_3",
    Base.metadata,
    Column("parent_id", ForeignKey("datasets.id"), primary_key=True),
    Column("child_id", ForeignKey("datasets.id"), primary_key=True),
)

association_table_4 = Table(
    "association_table_4",
    Base.metadata,
    Column("file_id", ForeignKey("files.id"), primary_key=True),
    Column("energy_id", ForeignKey("energies.id"), primary_key=True),
)


class Dataset(Base):
    """Dataset corresponding to fatfind nickname."""

    __tablename__ = "datasets"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30), index=True, unique=True)
    description: Mapped[str]
    recid: Mapped[int]
    version: Mapped[str] = mapped_column(String(5))
    channel: Mapped[str] = mapped_column(String(10))
    format: Mapped[str] = mapped_column(String(10))
    status: Mapped[Status]
    data: Mapped[bool] = mapped_column(Boolean)
    files: Mapped[set[File]] = relationship(
        secondary=association_table_1,
        back_populates="datasets",
    )
    years: Mapped[set[Year]] = relationship(
        secondary=association_table_2,
        back_populates="datasets",
    )
    parents: Mapped[set[Dataset]] = relationship(
        secondary=association_table_3,
        primaryjoin=id == association_table_3.c.child_id,
        secondaryjoin=id == association_table_3.c.parent_id,
        back_populates="children",
    )
    children: Mapped[set[Dataset]] = relationship(
        secondary=association_table_3,
        primaryjoin=id == association_table_3.c.parent_id,
        secondaryjoin=id == association_table_3.c.child_id,
        back_populates="parents",
    )

    @property
    def title(self) -> str:
        """Title for OpenData portal."""
        if self.data:
            return f"DELPHI collision data {self.name}"

        return f"DELPHI simulation data {self.name}"
    
    @property
    def energies(self) -> list[Energy]:
        energies: set[Energy] = set()
        for f in self.files:
            for e in f.energies:
                energies.add(e)
        return list(energies)


    @property
    def entries(self) -> int:
        """Dataset number of entries."""
        return sum(f.entries for f in self.files)

    @property
    def events(self) -> int:
        """Dataset number of events."""
        return sum(f.events for f in self.files)

    @property
    def size(self) -> int:
        """Dataset size."""
        return sum(f.size for f in self.files)

    @property
    def first_year(self) -> str:
        return sorted(y.name for y in self.years)[0]
    
    def __repr__(self) -> str:
        """Dataset as String."""
        return f"Dataset(id={self.id}, name='{self.name}', recid={self.recid}, version='{self.version}')"


class File(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(primary_key=True)
    path: Mapped[str] = mapped_column(index=True, unique=True)
    size: Mapped[int]
    checksum: Mapped[int]
    entries: Mapped[int] = mapped_column(nullable=True)
    events: Mapped[int] = mapped_column(nullable=True)
    datasets: Mapped[set[Dataset]] = relationship(
        secondary=association_table_1,
        back_populates="files",
    )
    energies: Mapped[set[Energy]] = relationship(
        secondary=association_table_4,
        back_populates="files",
    )

    def __repr__(self) -> str:
        return f"File(id={self.id}, path='{self.path}', size={self.size})"

    @staticmethod
    def get_instance(session: Session, path: pathlib.Path) -> File | None:
        result = session.execute(select(File).where(File.path == str(path))).first()
        if result is not None:
            return result[0]

        try:
            size = path.stat().st_size
        except FileNotFoundError:
            return None

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
    datasets: Mapped[set[Dataset]] = relationship(
        secondary=association_table_2,
        back_populates="years",
    )

    def __repr__(self) -> str:
        return f"Year(id={self.id}, name={self.name})"

    @staticmethod
    def get_instance(session: Session, year: str) -> Year:
        result = session.execute(select(Year).where(Year.name == year)).first()
        if result is not None:
            return result[0]

        year_obj = Year(name=year)
        session.add(year_obj)
        return year_obj


class Energy(Base):
    __tablename__ = "energies"

    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[int] = mapped_column(Integer, index=True, unique=True)
    files: Mapped[set[File]] = relationship(
        secondary=association_table_4,
        back_populates="energies",
    )

    @staticmethod
    def get_instance(session: Session, energy: int) -> Energy | None:
        result = session.execute(select(Energy).where(Energy.value == energy)).first()
        if result is not None:
            return result[0]

        energy_obj = Energy(value=energy)
        session.add(energy_obj)
        return energy_obj

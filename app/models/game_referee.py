from uuid import UUID
import enum
from sqlalchemy import Integer, ForeignKey, Enum as SQLEnum, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class RefereeRole(str, enum.Enum):
    """Referee role in a match."""
    main = "main"
    first_assistant = "first_assistant"
    second_assistant = "second_assistant"
    fourth_referee = "fourth_referee"
    var_main = "var_main"
    var_assistant = "var_assistant"
    match_inspector = "match_inspector"


class GameReferee(Base):
    """Association between game and referee with their role."""
    __tablename__ = "game_referees"
    __table_args__ = (
        UniqueConstraint("game_id", "referee_id", "role", name="uq_game_referee_role"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("games.id"), nullable=False, index=True
    )
    referee_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("referees.id"), nullable=False, index=True
    )
    role: Mapped[RefereeRole] = mapped_column(
        SQLEnum(RefereeRole, name='refereerole', create_type=False),
        nullable=False
    )

    # Relationships
    game: Mapped["Game"] = relationship("Game", back_populates="referees")
    referee: Mapped["Referee"] = relationship("Referee", back_populates="game_assignments")

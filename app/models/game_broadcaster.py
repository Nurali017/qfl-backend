from sqlalchemy import Integer, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.sql_types import GAME_ID_SQL_TYPE


class GameBroadcaster(Base):
    """Association between game and broadcaster."""
    __tablename__ = "game_broadcasters"
    __table_args__ = (
        UniqueConstraint("game_id", "broadcaster_id", name="uq_game_broadcaster"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[int] = mapped_column(
        GAME_ID_SQL_TYPE, ForeignKey("games.id", ondelete="CASCADE"), nullable=False, index=True
    )
    broadcaster_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("broadcasters.id", ondelete="CASCADE"), nullable=False
    )
    sort_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False, server_default="0")

    # Relationships
    game: Mapped["Game"] = relationship("Game", back_populates="broadcasters")
    broadcaster: Mapped["Broadcaster"] = relationship("Broadcaster", back_populates="game_assignments")

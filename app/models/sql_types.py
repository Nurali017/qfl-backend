from sqlalchemy import BigInteger, Integer

# PostgreSQL uses BIGINT, SQLite tests need INTEGER for autoincrement PK behavior.
PLAYER_ID_SQL_TYPE = BigInteger().with_variant(Integer, "sqlite")
GAME_ID_SQL_TYPE = BigInteger().with_variant(Integer, "sqlite")

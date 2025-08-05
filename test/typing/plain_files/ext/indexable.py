from __future__ import annotations

from datetime import date
from typing import Dict
from typing import List

from sqlalchemy import ARRAY
from sqlalchemy import JSON
from sqlalchemy import select
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.indexable import index_property
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class Article(Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(primary_key=True)

    tags: Mapped[Dict[str, str]] = mapped_column(JSON)
    topic: hybrid_property[str] = index_property("tags", "topic")

    updates: Mapped[List[date]] = mapped_column(ARRAY[date])
    created_at = index_property(
        "updates", 0, mutable=True, default=date.today()
    )
    updated_at: hybrid_property[date] = index_property("updates", -1)


a = Article(
    tags={"topic": "database", "subject": "programming"},
    updates=[date(2025, 7, 28), date(2025, 7, 29)],
)

# EXPECTED_TYPE: str
reveal_type(a.topic)

# EXPECTED_RE_TYPE: sqlalchemy.*._HybridClassLevelAccessor\[builtins.str\*?\]
reveal_type(Article.topic)

# EXPECTED_TYPE: date
reveal_type(a.created_at)

# EXPECTED_TYPE: date
reveal_type(a.updated_at)

a.created_at = date(2025, 7, 30)

# EXPECTED_RE_TYPE: sqlalchemy.*._HybridClassLevelAccessor\[datetime.date\*?\]
reveal_type(Article.created_at)

# EXPECTED_RE_TYPE: sqlalchemy.*._HybridClassLevelAccessor\[datetime.date\*?\]
reveal_type(Article.updated_at)

stmt = select(Article.id, Article.topic, Article.created_at).where(
    Article.id == 1
)

# EXPECTED_RE_TYPE: .*Select\[.*int, .*str, datetime\.date\]
reveal_type(stmt)

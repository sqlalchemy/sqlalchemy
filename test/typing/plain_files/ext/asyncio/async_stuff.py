from asyncio import current_task

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_scoped_session
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine


engine = create_async_engine("")
SM = async_sessionmaker(engine, class_=AsyncSession)

async_session = AsyncSession(engine)

as_session = async_scoped_session(SM, current_task)


async def go() -> None:
    r = await async_session.scalars(text("select 1"), params=[])
    r.first()
    sr = await async_session.stream_scalars(text("select 1"), params=[])
    await sr.all()
    r = await as_session.scalars(text("select 1"), params=[])
    r.first()
    sr = await as_session.stream_scalars(text("select 1"), params=[])
    await sr.all()

    async with engine.connect() as conn:
        cr = await conn.scalars(text("select 1"))
        cr.first()
        scr = await conn.stream_scalars(text("select 1"))
        await scr.all()

    ast = async_session.get_transaction()
    if ast:
        ast.is_active
    nt = async_session.get_nested_transaction()
    if nt:
        nt.is_active

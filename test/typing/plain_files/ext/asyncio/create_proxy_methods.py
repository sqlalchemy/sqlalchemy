from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_scoped_session
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.session import async_sessionmaker

# async engine
async_engine: AsyncEngine = create_async_engine("")
async_engine.clear_compiled_cache()
async_engine.update_execution_options()
async_engine.get_execution_options()
async_engine.url
async_engine.pool
async_engine.dialect
async_engine.engine
async_engine.name
async_engine.driver
async_engine.echo


# async connection
async def go_async_conn() -> None:
    async_conn: AsyncConnection = await async_engine.connect()
    async_conn.closed
    async_conn.invalidated
    async_conn.dialect
    async_conn.default_isolation_level


# async session
AsyncSession.object_session(object())
AsyncSession.identity_key()
async_session: AsyncSession = AsyncSession(async_engine)
in_: bool = "foo" in async_session
list(async_session)
async_session.add(object())
async_session.add_all([])
async_session.expire(object())
async_session.expire_all()
async_session.expunge(object())
async_session.expunge_all()
async_session.get_bind()
async_session.is_modified(object())
async_session.in_transaction()
async_session.in_nested_transaction()
async_session.dirty
async_session.deleted
async_session.new
async_session.identity_map
async_session.is_active
async_session.autoflush
async_session.no_autoflush
async_session.info


# async scoped session
async def test_async_scoped_session() -> None:
    async_scoped_session.object_session(object())
    async_scoped_session.identity_key()
    await async_scoped_session.close_all()
    asm = async_sessionmaker()
    async_ss = async_scoped_session(asm, lambda: 42)
    value: bool = "foo" in async_ss
    print(value)
    list(async_ss)
    async_ss.add(object())
    async_ss.add_all([])
    async_ss.begin()
    async_ss.begin_nested()
    await async_ss.close()
    await async_ss.commit()
    await async_ss.connection()
    await async_ss.delete(object())
    await async_ss.execute(text("select 1"))
    async_ss.expire(object())
    async_ss.expire_all()
    async_ss.expunge(object())
    async_ss.expunge_all()
    await async_ss.flush()
    await async_ss.get(object, 1)
    async_ss.get_bind()
    async_ss.is_modified(object())
    await async_ss.merge(object())
    await async_ss.refresh(object())
    await async_ss.rollback()
    await async_ss.scalar(text("select 1"))
    async_ss.bind
    async_ss.dirty
    async_ss.deleted
    async_ss.new
    async_ss.identity_map
    async_ss.is_active
    async_ss.autoflush
    async_ss.no_autoflush
    async_ss.info

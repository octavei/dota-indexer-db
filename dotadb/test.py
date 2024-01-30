from sqlalchemy import create_engine, Column, Integer, String, Table, MetaData, UniqueConstraint

engine = create_engine('sqlite:///:memory:')
metadata = MetaData()

# 定义表，并在这里添加 UniqueConstraint
my_table = Table(
    'my_table',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('cl', String),
    UniqueConstraint('name', 'cl', name='uq_name_class')  # 联合唯一约束
)

metadata.create_all(engine)

from sqlalchemy.orm import Session

# 插入重复记录，将会引发唯一性约束冲突异常
with Session(engine) as session:
    session.execute(my_table.insert().values(id=1, name='John', cl='A'))
    session.execute(my_table.insert().values(id=2, name='Jane', cl='B'))
    session.execute(my_table.insert().values(id=3, name='John', cl='A'))  # 这一行将会引发异常



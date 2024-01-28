from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, text, CheckConstraint, BigInteger, DECIMAL, DateTime
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.dialects.mysql import insert


class DotaDB:
    def __init__(self, db_url):
        self.engine = create_engine(db_url, echo=True)
        self.metadata = MetaData()
        self.session = Session(bind=self.engine)
        # self.metadata.create_all(bind=self.engine)

    def __table(self, table_name: str):
        return Table(table_name, self.metadata,
                     Column('id', Integer, primary_key=True),
                     Column('name', String(255)),
                     )

    def create_table(self, table_name):
        # 定义表格
        table = self.__table(table_name)
        # 创建表格
        self.metadata.create_all(bind=self.engine)

    # def insert_or_update(self, ):
    #     with self.session.begin():
    #         stmt = insert(table).values(id=id, name=name)
    #         stmt = stmt.on_duplicate_key_update(name=stmt.inserted.name)
    #         self.session.execute(stmt)

    # def select(self):
    #     se = self.table.select()
    #     result = self.session.execute(se).fetchall()
    #     print("r:", result)
    #
    # def delete(self, table_name: str):
    #     self.session.execute(self.table.delete())

    def close(self):
        self.session.close()


if __name__ == "__main__":
    url = 'mysql+mysqlconnector://root:116000@localhost/wjy'
    db = DotaDB(url)
    db.create_table("haha")
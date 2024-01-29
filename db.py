from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, text, CheckConstraint, BigInteger, \
    DECIMAL, DateTime
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.dialects.mysql import insert


class DotaDB:
    def __init__(self, db_url):
        self.engine = create_engine(db_url, echo=True)
        self.metadata = MetaData()
        self.p = "dot-20"
        self.session = Session(bind=self.engine)
        self.deploy_table = self._deploy_table()
        self.metadata.create_all(bind=self.engine)

    # 直接一个资产表 程序启动直接创建
    def _currency_table(self, tick: str):
        return Table(tick + "_currency", self.metadata,
                     Column('user', String(64), nullable=False, primary_key=True),
                     Column('tick', String(8), nullable=False, server_default=tick, primary_key=True),
                     Column('balance', DECIMAL(46, 18), default=0, nullable=False),
                     )

    # 部署表  程序启动直接创建
    def _deploy_table(self):
        return Table("deploy", self.metadata,
                     Column('id', Integer, autoincrement=True),
                     Column("deployer", String(64), nullable=False),

                     # 用于标记这个部署事件在链上哪个区块高度哪个位置
                     Column("block_height", Integer, nullable=False),
                     Column("block_hash", String(64), nullable=False),
                     Column("extrinsic_index", Integer, nullable=False),
                     Column("batchall_index", Integer, default=0),
                     Column("remark_index", Integer, default=0),

                     Column("p", String(8), server_default=self.p, nullable=False),
                     Column('op', String(16), server_default="deploy", nullable=False),
                     Column("tick", String(8), primary_key=True, nullable=False),
                     Column('decimal', Integer, default=18),
                     Column("mode", String(8), default="fair", nullable=False),
                     Column("amt", DECIMAL(46, 18), default=0),
                     Column("start", Integer, default=0),
                     Column("end", Integer, default=0),
                     Column("max", DECIMAL(46, 18), default=0),
                     Column("lim", DECIMAL(46, 18), default=0),
                     Column("admin", String(64)),
                     )

    def get_deploy_info(self, tick: str):
        se = self.deploy_table.select().where(self.deploy_table.c.tick == tick)
        result = self.session.execute(se).fetchall()
        return result

    def insert_deploy_info(self, deploy_info: dict):
        with self.session.begin():
            stmt = insert(self.deploy_table).values(**deploy_info)
            self.session.execute(stmt)

    # mint table
    def _mint_table(self, tick: str):
        return Table(tick + "_mint", self.metadata,
                     Column('id', Integer, autoincrement=True),

                     Column("singer", String(64), nullable=False, primary_key=True),
                     Column("block_height", Integer, nullable=False, primary_key=True),
                     Column("block_hash", String(64), nullable=False),
                     Column("extrinsic_index", Integer, nullable=False, primary_key=True),
                     Column("batchall_index", Integer, default=0, primary_key=True),
                     Column("remark_index", Integer, default=0, primary_key=True),

                     Column("p", String(8), server_default=self.p, nullable=False),
                     Column('op', String(16), server_default="mint", nullable=False),
                     Column("tick", String(8), server_default=tick, nullable=False),
                     Column("to", String(64), nullable=False),
                     Column("lim", DECIMAL(46, 18), default=0),
                     )

    def insert_mint_info(self, tick: str, mint_infos: list[dict]):
        with self.session.begin():
            for mint_info in mint_infos:
                if mint_info.get("tick") != tick:
                    raise Exception("tick is None or not equal")
                if mint_info.get("lim") == 0:
                    raise Exception("lim is 0")
                stmt = insert(self._mint_table(mint_info["tick"])).values(**mint_info)
                self.session.execute(stmt)

    def create_tables_for_new_tick(self, tick: str):
        # 创建currency表
        currency_table = self._currency_table(tick)
        # 创建mint表
        mint_table = self._mint_table(tick)
        self.metadata.create_all(bind=self.engine)

    def insert_or_update_user_currency_balance(self, balance_info: dict):
        if balance_info.get("tick") is None:
            raise Exception("tick is None")
        with self.session.begin():
            table = self._currency_table(balance_info["tick"])
            stmt = insert(table).values(balance_info)
            stmt = stmt.on_duplicate_key_update(balance_info)
            self.session.execute(stmt)

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
    db.create_tables_for_new_tick("dota")

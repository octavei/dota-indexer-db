import time

from sqlalchemy import create_engine, Column, Integer, String, \
    MetaData, Table, text, CheckConstraint, BigInteger, \
    DECIMAL, DateTime, func, UniqueConstraint

from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.dialects.mysql import insert
# import dotadb
#
# __all__ = ["DotaDB"]


class DotaDB:
    def __init__(self, db_url):
        self.engine = create_engine(db_url, echo=True)
        self.metadata = MetaData()
        self.p = "dot-20"
        self.session = Session(bind=self.engine)
        self.deploy_table = self._deploy_table()
        self.indexer_status_table = self._indexer_status_table()
        self.metadata.create_all(bind=self.engine)

    # tick资产表
    def _currency_table(self, tick: str):
        return Table(tick + "_currency", self.metadata,
                     Column('user', String(64), nullable=False, primary_key=True),
                     Column('tick', String(8), nullable=False, default=tick, primary_key=True),
                     Column('balance', DECIMAL(46, 18), nullable=False),
                     extend_existing=True
                     )

    # tick索引器状态表（更新到了哪个高度）状态表不能区分tick来爬 因为token之间完全是有可能相互交互的
    def _indexer_status_table(self):
        return Table("indexer_status", self.metadata,
                     # 要测这个是否是不可更
                     Column('p', String(8), primary_key=True, default=self.p, nullable=False,
                            server_onupdate=text(self.p)),
                     Column('indexer_height', Integer, nullable=False),
                     Column('crawler_height', Integer, nullable=False),
                     extend_existing=True
                     )

    def _approve_table(self, tick: str):
        return Table(tick + "_approve", self.metadata,
                     Column('id', Integer, autoincrement=True, primary_key=True),
                     Column("user", String(64), nullable=False, primary_key=True),
                     Column("from_address", String(64), nullable=False, primary_key=True),
                     Column("tick", String(8), default=tick, nullable=False),
                     Column("amount", DECIMAL(46, 18), nullable=False),
                     UniqueConstraint("user", "from_address"),
                     extend_existing=True
                     )

    # # 获取整个approve表格数据
    # def get_all_approve_info(self, tick: str):
    #     se = self._approve_table(tick).select()
    #     return self.session.execute(se).fetchall()

    # 更新或者插入用户授权记录
    def insert_or_update_user_approve(self, tick: str, approve_infos: list[dict]):
        with self.session.begin_nested():
            for approve_info in approve_infos:
                if approve_info.get("tick") != tick:
                    raise Exception("tick is None or not equal")
                stmt = insert(self._approve_table(tick)).values(**approve_info)
                stmt = stmt.on_duplicate_key_update(approve_info)
                self.session.execute(stmt)

    # 授权历史记录表
    def _approve_history_table(self, tick: str):
        return Table(tick + "_approve_history", self.metadata,
                     Column('id', Integer, autoincrement=True, primary_key=True),
                     Column("user", String(64), nullable=False, primary_key=True),
                     Column("from", String(64), nullable=False, primary_key=True),
                     Column("tick", String(8), default=tick, nullable=False),
                     Column("amount", DECIMAL(46, 18), nullable=False),
                     
                     Column("block_height", Integer, nullable=False, primary_key=True),
                     Column("block_hash", String(66), nullable=False),
                     Column("extrinsic_index", Integer, nullable=False, primary_key=True),
                     Column("batchall_index", Integer, nullable=False, primary_key=True),
                     Column("remark_index", Integer, nullable=False, primary_key=True),
                     UniqueConstraint("block_height", "extrinsic_index", "batchall_index", "remark_index"),
                     extend_existing=True
                     )

    # 插入授权记录
    def insert_approve_history(self, tick: str, approve_infos: list[dict]):
        with self.session.begin_nested():
            for approve_info in approve_infos:
                if approve_info.get("tick") != tick:
                    raise Exception("tick is None or not equal")
                stmt = insert(self._approve_history_table(tick)).values(**approve_info)
                self.session.execute(stmt)

    # 获取授权金额
    def get_user_approve_amount(self, tick: str, user: str, from_: str):
        table = self._approve_table(tick)
        se = table.select().where(table.c.user == user).where(table.c.from_address == from_)
        result = self.session.execute(se).fetchone()
        return result

    # 插入或者更新索引器状态
    def insert_or_update_indexer_status(self, status: dict):
        with self.session.begin_nested():
            stmt = insert(self._indexer_status_table()).values(**status)
            stmt = stmt.on_duplicate_key_update(status)
            self.session.execute(stmt)

    # 获取索引器状态
    def get_indexer_status(self, p: str):
        se = self._indexer_status_table().select().where(self._indexer_status_table().c.p == p)
        result = self.session.execute(se).fetchone()
        return result

    # 获取用户tick资产
    def get_user_currency_balance(self, tick: str, user: str):
        table = self._currency_table(tick)
        se = table.select().where(table.c.user == user)
        result = self.session.execute(se).fetchone()
        return result

    # 获取tick总发行量
    def get_total_supply(self, tick: str):
        table = self._currency_table(tick)
        se = func.sum(table.c.balance)
        result = self.session.execute(se).fetchone()
        return result

    # 更新（或者插入）用户tick资产
    def insert_or_update_user_currency_balance(self, tick: str, balance_infos: list[dict]):

        with self.session.begin_nested():
            for balance_info in balance_infos:
                if balance_info.get("tick") != tick:
                    raise Exception("tick is None or not equal")
                # with self.session.begin_nested():
                table = self._currency_table(balance_info["tick"])
                stmt = insert(table).values(balance_info)
                stmt = stmt.on_duplicate_key_update(balance_info)
                self.session.execute(stmt)

    def _transfer_table(self, tick: str):
        return Table(tick + "_transfer", self.metadata,
                     Column('id', Integer, autoincrement=True, primary_key=True),

                     Column("user", String(64), nullable=False),

                     Column("block_height", Integer, nullable=False, primary_key=True),
                     Column("block_hash", String(66), nullable=False),
                     Column("extrinsic_index", Integer, nullable=False, primary_key=True),
                     Column("extrinsic_hash", String(66), nullable=False),
                     Column("batchall_index", Integer, nullable=False,primary_key=True),
                     Column("remark_index", Integer, nullable=False,primary_key=True),

                     Column("amount", DECIMAL(46, 18), nullable=False),
                     Column("from", String(64), nullable=False, primary_key=True),
                     Column("to", String(64), nullable=False, primary_key=True),
                     Column("tick", String(8), default=tick, nullable=False),
                     Column("type", Integer, default=0, nullable=False), # 0是transfer 1是transferFrom
                     UniqueConstraint("block_height", "extrinsic_index", "batchall_index", "remark_index"),
                     extend_existing=True
                     )

    # 插入用户tick转账记录
    def insert_transfer_info(self, tick: str, transfer_infos: list[dict]):
        with self.session.begin_nested():
            for transfer_info in transfer_infos:
                if transfer_info.get("tick") != tick:
                    raise Exception("tick is None or not equal")
                if transfer_info.get("amount") == 0:
                    raise Exception("amount is 0")
                stmt = insert(self._transfer_table(tick)).values(**transfer_info)
                self.session.execute(stmt)

    # 部署表
    def _deploy_table(self):
        return Table("deploy", self.metadata,
                     Column('id', Integer, autoincrement=True, primary_key=True),
                     Column("deployer", String(64), nullable=False, primary_key=True),

                     # 用于标记这个部署事件在链上哪个区块高度哪个位置
                     Column("block_height", Integer, nullable=False, primary_key=True),
                     Column("block_hash", String(66), nullable=False),
                     Column("extrinsic_index", Integer, nullable=False, primary_key=True),
                     Column("batchall_index", Integer, nullable=False, primary_key=True),
                     Column("remark_index", Integer, nullable=False, primary_key=True),

                     Column("p", String(8), default=self.p, nullable=False),
                     Column('op', String(16), default="deploy", nullable=False),
                     Column("tick", String(8), primary_key=True, nullable=False),
                     Column('decimal', Integer, default=18),
                     Column("mode", String(8), default="fair", nullable=False),
                     Column("amt", DECIMAL(46, 18)),
                     Column("start", Integer, nullable=False),
                     Column("end", Integer),
                     Column("max", DECIMAL(46, 18)),
                     Column("lim", DECIMAL(46, 18)),
                     Column("admin", String(64)),
                     UniqueConstraint("block_height", "extrinsic_index", "batchall_index", "remark_index"),
                     extend_existing=True
                     )

    # 获取tick部署信息
    def get_deploy_info(self, tick: str):
        se = self.deploy_table.select().where(self.deploy_table.c.tick == tick)
        result = self.session.execute(se).fetchall()
        return result

    # 插入部署信息
    def insert_deploy_info(self, deploy_info: dict):
        with self.session.begin_nested():
            stmt = insert(self.deploy_table).values(**deploy_info)
            self.session.execute(stmt)

    # mint table
    def _mint_table(self, tick: str):
        return Table(tick + "_mint", self.metadata,
                     Column('id', Integer, autoincrement=True, primary_key=True),

                     Column("singer", String(64), nullable=False, primary_key=True),
                     Column("block_height", Integer, nullable=False, primary_key=True),
                     Column("block_hash", String(66), nullable=False),
                     Column("extrinsic_index", Integer, nullable=False, primary_key=True),
                     Column("extrinsic_hash", String(66), nullable=False),
                     Column("batchall_index", Integer, default=0, primary_key=True),
                     Column("remark_index", Integer, default=0, primary_key=True),

                     Column("p", String(8), default=self.p, nullable=False),
                     Column('op', String(16), default="mint", nullable=False),
                     Column("tick", String(8), default=tick, nullable=False),
                     Column("to", String(64), nullable=False, primary_key=True),
                     Column("lim", DECIMAL(46, 18), default=0),
                     UniqueConstraint("block_height", "extrinsic_index", "batchall_index", "remark_index"),
                     extend_existing=True
                     )

    # 插入用户mint记录
    def insert_mint_info(self, tick: str, mint_infos: list[dict]):
        with self.session.begin_nested():
            for mint_info in mint_infos:
                if mint_info.get("tick") != tick:
                    raise Exception("tick is None or not equal")
                if mint_info.get("lim") == 0:
                    raise Exception("lim is 0")
                stmt = insert(self._mint_table(mint_info["tick"])).values(**mint_info)
                self.session.execute(stmt)

    # 部署成功后 给tick添加对应的表
    def create_tables_for_new_tick(self, tick: str):
        # 创建currency表
        currency_table = self._currency_table(tick)
        # 创建mint表
        mint_table = self._mint_table(tick)
        approve_table = self._approve_table(tick)
        approve_history_table = self._approve_history_table(tick)
        transfer_table = self._transfer_table(tick)
        self.deploy_table = self._deploy_table()
        self.indexer_status_table = self._indexer_status_table()
        self.metadata.create_all(bind=self.engine)

    # def select(self):
    #     se = self.table.select()
    #     result = self.session.execute(se).fetchall()
    #     print("r:", result)
    #

    # 删除所有tick有关的表格中的数据
    def delete_all_tick_table(self, tick: str):
        # 创建currency表
        with self.session.begin_nested():
            currency_table = self._currency_table(tick)
            # 创建mint表
            mint_table = self._mint_table(tick)
            approve_table = self._approve_table(tick)
            approve_history_table = self._approve_history_table(tick)
            transfer_table = self._transfer_table(tick)
            self.session.execute(currency_table.delete())
            self.session.execute(mint_table.delete())
            self.session.execute(approve_table.delete())
            self.session.execute(approve_history_table.delete())
            self.session.execute(transfer_table.delete())
            self.session.execute(self.indexer_status_table.delete())
            self.session.execute(self.deploy_table.delete())

    # drop所有tick有关的表格
    def drop_all_tick_table(self, tick: str):
        # 创建currency表
        # with self.session.begin_nested():
        self._currency_table(tick).drop(bind=self.engine)
        # 创建mint表
        self._mint_table(tick).drop(bind=self.engine)
        self._approve_table(tick).drop(bind=self.engine)
        self._approve_history_table(tick).drop(bind=self.engine)
        self._transfer_table(tick).drop(bind=self.engine)
        # self.session.execute(currency_table.delete())
        # self.session.execute(mint_table.delete())
        # self.session.execute(approve_table.delete())
        # self.session.execute(approve_history_table.delete())
        # self.session.execute(transfer_table.delete())
        self.indexer_status_table.drop(bind=self.engine)
        self.deploy_table.drop(bind=self.engine)

    def close(self):
        self.session.close()


if __name__ == "__main__":
    url = 'mysql+mysqlconnector://root:116000@localhost/wjy'
    db = DotaDB(url)
    try:
        db.drop_all_tick_table("dota")
    except Exception as e:
        print(e)
    time.sleep(5)
    db.session.commit()
    db.create_tables_for_new_tick("dota")

    # db.insert_or_update_user_currency_balance("dota", [{"user": "1", "balance": 1, "tick": "dota"}])
    # print("#####"*100)
    # db.session.commit()
    #
    # try:
    #     with db.session.begin():
    #         db.insert_or_update_indexer_status({"indexer_height": 2, "crawler_height": 100})
    #         # 如果是begin 都会回滚
    #         # 如果是begin_nested 外层不会回滚
    #         # begin_nested内层raise 外层不一定回滚 只有这个raise到外层 才会回滚
    #         # begin 只要内层出现raise 不管处不处理 整个begin都回滚
    #         db.insert_or_update_user_currency_balance("dota", [{"user": "82", "balance": 100, "tick": "dota"}])
    #         # try:
    #         #     db.insert_or_update_user_currency_balance("dota", [{"user": "92", "balance": "haha", "tick": "dota"}])
    #         # except Exception as e:
    #         #     print(e)
    # except Exception as e:
    #     print("err:", e)
    #     print("***"*100)
    # total = db.get_total_supply("dota")
    # amt = db.get_user_currency_balance("dota", "82")
    # print(total)
    # print(amt)
    # print("###"*100)
    # print(db.get_indexer_status("dot-20"))
    #
    db.session.commit()
    #
    with db.session.begin():
        db.insert_or_update_user_approve("dota", [{"user": "sss", "from_address": "ddd", "tick": "dota", "amount": 100}])
        db.insert_or_update_user_approve("dota", [{"user": "sss", "from_address": "ddd", "tick": "dota", "amount": 10}])
        db.insert_or_update_user_approve("dota", [{"user": "ssss", "from_address": "ddd", "tick": "dota", "amount": 20}])

    print(db.get_user_approve_amount("dota", "ssss", "ddd"))
    print(db.get_all_approve_info("dota"))
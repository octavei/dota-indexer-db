from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, text, CheckConstraint, BigInteger, \
    DECIMAL, DateTime, func
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

    # tick资产表
    def _currency_table(self, tick: str):
        return Table(tick + "_currency", self.metadata,
                     Column('user', String(64), nullable=False, primary_key=True),
                     Column('tick', String(8), nullable=False, server_default=tick, primary_key=True),
                     Column('balance', DECIMAL(46, 18), default=0, nullable=False),
                     extend_existing=True
                     )

    # tick索引器状态表（更新到了哪个高度）状态表不能区分tick来爬 因为token之间完全是有可能相互交互的
    def _indexer_status_table(self):
        return Table("indexer_status", self.metadata,
                     # 要测这个是否是不可更
                     Column('p', String(8), primary_key=True, server_default=self.p, nullable=False),
                     Column('indexer_height', Integer, nullable=False, default=0),
                     Column('crawler_height', Integer, nullable=False, default=0),
                     extend_existing=True
                     )

    def _approve_table(self, tick: str):
        return Table(tick + "_approve", self.metadata,
                     Column('id', Integer, autoincrement=True),
                     Column("user", String(64), nullable=False, primary_key=True),
                     Column("from", String(64), nullable=False, primary_key=True),
                     Column("tick", String(8), server_default=tick, nullable=False, primary_key=True),
                     Column("amount", DECIMAL(46, 18), default=0, nullable=False),
                     extend_existing=True
                     )

    # 更新或者插入用户授权记录
    def insert_or_update_user_approve(self, tick: str, approve_infos: list[dict]):
        with self.session.begin_nested():
            for approve_info in approve_infos:
                if approve_info.get("tick") != tick:
                    raise Exception("tick is None or not equal")
                if approve_info.get("amount") == 0:
                    raise Exception("amount is 0")
                stmt = insert(self._approve_table(tick)).values(**approve_info)
                stmt = stmt.on_duplicate_key_update(approve_info)
                self.session.execute(stmt)

    # 授权历史记录表
    def _approve_history_table(self, tick: str):
        return Table(tick + "_approve_history", self.metadata,
                     Column('id', Integer, autoincrement=True),
                     Column("user", String(64), nullable=False, primary_key=True),
                     Column("from", String(64), nullable=False, primary_key=True),
                     Column("tick", String(8), server_default=tick, nullable=False, primary_key=True),
                     Column("amount", DECIMAL(46, 18), default=0, nullable=False),
                     Column("block_height", Integer, nullable=False),
                     Column("block_hash", String(64), nullable=False),
                     Column("extrinsic_index", Integer, nullable=False),
                     Column("batchall_index", Integer, default=0),
                     Column("remark_index", Integer, default=0),
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
        se = table.select().where(table.c.user == user).where(table.c.from_ == from_)
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
                     Column('id', Integer, autoincrement=True),

                     Column("user", String(64), nullable=False),

                     Column("block_height", Integer, nullable=False, primary_key=True),
                     Column("block_hash", String(64), nullable=False, primary_key=True),
                     Column("extrinsic_index", Integer, nullable=False, primary_key=True),
                     Column("extrinsic_hash", String(64), nullable=False, primary_key=True),
                     Column("batchall_index", Integer, default=0, primary_key=True),
                     Column("remark_index", Integer, default=0, primary_key=True),

                     Column("amount", DECIMAL(46, 18), default=0, nullable=False),
                     Column("from", String(64), nullable=False),
                     Column("to", String(64), nullable=False),
                     Column("tick", String(8), server_default=tick, nullable=False),
                     Column("amt", DECIMAL(46, 18), default=0),
                     Column("type", Integer, default=0, nullable=False), # 0是transfer 1是transferFrom
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
                     Column('id', Integer, autoincrement=True),

                     Column("singer", String(64), nullable=False, primary_key=True),
                     Column("block_height", Integer, nullable=False, primary_key=True),
                     Column("block_hash", String(64), nullable=False),
                     Column("extrinsic_index", Integer, nullable=False, primary_key=True),
                     Column("extrinsic_hash", String(64), nullable=False),
                     Column("batchall_index", Integer, default=0, primary_key=True),
                     Column("remark_index", Integer, default=0, primary_key=True),

                     Column("p", String(8), server_default=self.p, nullable=False),
                     Column('op', String(16), server_default="mint", nullable=False),
                     Column("tick", String(8), server_default=tick, nullable=False),
                     Column("to", String(64), nullable=False),
                     Column("lim", DECIMAL(46, 18), default=0),
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
        self.metadata.create_all(bind=self.engine)

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
    # db.insert_or_update_user_currency_balance({"user": "1", "balance": 1, "tick": "dota"})
    try:
        with db.session.begin():
            # 如果是begin 都会回滚
            # 如果是begin_nested 外层不会回滚
            # begin_nested内层raise 外层不一定回滚 只有这个raise到外层 才会回滚
            # begin 只要内层出现raise 不管处不处理 整个begin都回滚
            db.insert_or_update_user_currency_balance("dota", [{"user": "82", "balance": 100, "tick": "dota"}])
            try:
                db.insert_or_update_user_currency_balance("dota", [{"user": "92", "balance": "haha", "tick": "dota"}])
            except Exception as e:
                print(e)
    except Exception as e:
        print("err:", e)
    total = db.get_total_supply("dota")
    amt = db.get_user_currency_balance("dota", "82")
    print(total)
    print(amt)

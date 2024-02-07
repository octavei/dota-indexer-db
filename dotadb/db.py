import time

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, Column, Integer, String, \
    MetaData, Table, text, CheckConstraint, BigInteger, \
    DECIMAL, DateTime, func, UniqueConstraint

from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.dialects.mysql import insert


class DotaDB:
    def __init__(self, db_url):
        self.engine = create_engine(db_url, echo=True)
        self.metadata = MetaData()
        self.p = "dot-20"
        self.session = Session(bind=self.engine)
        self.deploy_table = self._deploy_table()
        self.indexer_status_table = self._indexer_status_table()
        self.metadata.create_all(bind=self.engine)

    def _currency_table(self, tick: str):
        return Table(tick + "_currency", self.metadata,
                     Column('user', String(64), nullable=False, primary_key=True),
                     Column('tick', String(8), nullable=False, default=tick, primary_key=True),
                     Column('balance', DECIMAL(64, 18), nullable=False),
                     extend_existing=True
                     )

    def _indexer_status_table(self):
        return Table("indexer_status", self.metadata,
                     Column('p', String(8), primary_key=True, default=self.p, nullable=False),
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
                     Column("amount", DECIMAL(64, 18), nullable=False),
                     UniqueConstraint("user", "from_address"),
                     extend_existing=True
                     )

    def insert_or_update_user_approve(self, tick: str, approve_infos: list[dict]):
        try:
            with self.session.begin_nested():
                for approve_info in approve_infos:
                    if approve_info.get("tick") != tick:
                        raise Exception("tick is None or not equal")
                    stmt = insert(self._approve_table(tick)).values(**approve_info)
                    stmt = stmt.on_duplicate_key_update(approve_info)
                    self.session.execute(stmt)
        except Exception as e:
            raise e

    def _approve_history_table(self, tick: str):
        return Table(tick + "_approve_history", self.metadata,
                     Column('id', Integer, autoincrement=True, primary_key=True),
                     Column("user", String(64), nullable=False, primary_key=True),
                     Column("from", String(64), nullable=False, primary_key=True),
                     Column("tick", String(8), default=tick, nullable=False),
                     Column("amount", DECIMAL(64, 18), nullable=False),
                     Column("memo_remark", String(1024), nullable=True),
                     
                     Column("block_height", Integer, nullable=False, primary_key=True),
                     Column("block_hash", String(66), nullable=False),
                     Column("extrinsic_index", Integer, nullable=False, primary_key=True),
                     Column("batchall_index", Integer, nullable=False, primary_key=True),
                     Column("remark_index", Integer, nullable=False, primary_key=True),
                     UniqueConstraint("block_height", "extrinsic_index", "batchall_index", "remark_index"),
                     extend_existing=True
                     )

    def insert_approve_history(self, tick: str, approve_infos: list[dict]):
        try:
            with self.session.begin_nested():
                for approve_info in approve_infos:
                    if approve_info.get("tick") != tick:
                        raise Exception("tick is None or not equal")
                    stmt = insert(self._approve_history_table(tick)).values(**approve_info)
                    self.session.execute(stmt)
        except Exception as e:
            raise e

    def get_user_approve_amount(self, tick: str, user: str, from_: str):
        table = self._approve_table(tick)
        se = table.select().where(table.c.user == user).where(table.c.from_address == from_)
        result = self.session.execute(se).fetchone()
        return result

    def insert_or_update_indexer_status(self, status: dict):
        try:
            with self.session.begin_nested():
                stmt = insert(self._indexer_status_table()).values(**status)
                stmt = stmt.on_duplicate_key_update(status)
                self.session.execute(stmt)
        except SQLAlchemyError as e:
            raise e

    def get_indexer_status(self, p: str):
        se = self._indexer_status_table().select().where(self._indexer_status_table().c.p == p)
        result = self.session.execute(se).fetchone()
        return result

    def get_user_currency_balance(self, tick: str, user: str):
        table = self._currency_table(tick)
        se = table.select().where(table.c.user == user)
        result = self.session.execute(se).fetchone()
        return result

    def get_total_supply(self, tick: str):
        table = self._currency_table(tick)
        se = func.sum(table.c.balance)
        result = self.session.execute(se).fetchone()
        return result

    def insert_or_update_user_currency_balance(self, tick: str, balance_infos: list[dict]):
        try:
            with self.session.begin_nested():
                for balance_info in balance_infos:
                    if balance_info.get("tick") != tick:
                        raise Exception("tick is None or not equal")
                    table = self._currency_table(balance_info["tick"])
                    stmt = insert(table).values(balance_info)
                    stmt = stmt.on_duplicate_key_update(balance_info)
                    self.session.execute(stmt)
        except Exception as e:
            raise e

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

                     Column("amount", DECIMAL(64, 18), nullable=False),
                     Column("from", String(64), nullable=False, primary_key=True),
                     Column("to", String(64), nullable=False, primary_key=True),
                     Column("tick", String(8), default=tick, nullable=False),
                     Column("type", Integer, default=0, nullable=False), # 0是transfer 1是transferFrom
                     Column("memo_remark", String(1024), nullable=True),
                     UniqueConstraint("block_height", "extrinsic_index", "batchall_index", "remark_index"),
                     extend_existing=True
                     )

    def insert_transfer_info(self, tick: str, transfer_infos: list[dict]):
        try:
            with self.session.begin_nested():
                for transfer_info in transfer_infos:
                    if transfer_info.get("tick") != tick:
                        raise Exception("tick is None or not equal")
                    if transfer_info.get("amount") == 0:
                        raise Exception("amount is 0")
                    stmt = insert(self._transfer_table(tick)).values(**transfer_info)
                    self.session.execute(stmt)
        except SQLAlchemyError as e:
            raise e

    def _deploy_table(self):
        return Table("deploy", self.metadata,
                     Column('id', Integer, autoincrement=True, primary_key=True),
                     Column("deployer", String(64), nullable=False, primary_key=True),

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
                     Column("amt", DECIMAL(64, 18)),
                     Column("start", Integer, nullable=False),
                     Column("end", Integer),
                     Column("max", DECIMAL(64, 18)),
                     Column("lim", DECIMAL(64, 18)),
                     Column("admin", String(64)),
                     Column("memo_remark", String(1024), nullable=True),
                     UniqueConstraint("block_height", "extrinsic_index", "batchall_index", "remark_index"),
                     extend_existing=True
                     )

    def get_deploy_info(self, tick: str):
        se = self.deploy_table.select().where(self.deploy_table.c.tick == tick)
        result = self.session.execute(se).fetchall()
        return result

    def insert_deploy_info(self, deploy_info: dict):
        try:
            with self.session.begin_nested():
                stmt = insert(self.deploy_table).values(**deploy_info)
                self.session.execute(stmt)
        except SQLAlchemyError as e:
            raise e

    def _mint_table(self, tick: str):
        return Table(tick + "_mint", self.metadata,
                     Column('id', Integer, autoincrement=True, primary_key=True),

                     Column("singer", String(64), nullable=False, primary_key=True),
                     Column("block_height", Integer, nullable=False, primary_key=True),
                     Column("block_hash", String(66), nullable=False),
                     Column("extrinsic_index", Integer, nullable=False, primary_key=True),
                     Column("extrinsic_hash", String(66), nullable=False),
                     Column("batchall_index", Integer, primary_key=True),
                     Column("remark_index", Integer, primary_key=True),

                     Column("p", String(8), default=self.p, nullable=False),
                     Column('op', String(16), default="mint", nullable=False),
                     Column("tick", String(8), default=tick, nullable=False),
                     Column("to", String(64), nullable=False, primary_key=True),
                     Column("lim", DECIMAL(64, 18)),
                     Column("memo_remark", String(1024), nullable=True),
                     UniqueConstraint("block_height", "extrinsic_index", "batchall_index", "remark_index"),
                     extend_existing=True
                     )

    def insert_mint_info(self, tick: str, mint_infos: list[dict]):
        try:
            with self.session.begin_nested():
                for mint_info in mint_infos:
                    if mint_info.get("tick") != tick:
                        raise Exception("tick is None or not equal")
                    if mint_info.get("lim") == 0:
                        raise Exception("lim is 0")
                    stmt = insert(self._mint_table(mint_info["tick"])).values(**mint_info)
                    self.session.execute(stmt)
        except Exception as e:
            raise e

    def create_tables_for_new_tick(self, tick: str):
        try:
            currency_table = self._currency_table(tick)
            mint_table = self._mint_table(tick)
            approve_table = self._approve_table(tick)
            approve_history_table = self._approve_history_table(tick)
            transfer_table = self._transfer_table(tick)
            self.deploy_table = self._deploy_table()
            self.indexer_status_table = self._indexer_status_table()
            self.metadata.create_all(bind=self.engine)
        except SQLAlchemyError as e:
            raise e

    def delete_all_tick_table(self, tick: str):
        with self.session.begin_nested():
            currency_table = self._currency_table(tick)
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

    def drop_all_tick_table(self, tick: str):
        try:
            self._currency_table(tick).drop(bind=self.engine)
        except Exception as e:
            pass
        try:
            self._mint_table(tick).drop(bind=self.engine)
        except Exception as e:
            pass

        try:
            self._approve_table(tick).drop(bind=self.engine)
        except Exception as e:
            pass
        try:
            self._approve_history_table(tick).drop(bind=self.engine)
        except Exception as e:
            pass
        try:
            self._transfer_table(tick).drop(bind=self.engine)
        except Exception as e:
            pass
        try:
            self.indexer_status_table.drop(bind=self.engine)
        except Exception as e:
            pass
        try:
            self.deploy_table.drop(bind=self.engine)
        except Exception as e:
            pass

    def close(self):
        self.session.close()


if __name__ == "__main__":
    pass

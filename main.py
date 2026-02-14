import sqlite3
import random
import time
import asyncio
import os
from typing import Optional, List, Tuple
from contextlib import contextmanager

from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register, StarTools
from astrbot.api import logger, AstrBotConfig
from astrbot.api.message_components import Node, Nodes, Plain


class CaveDatabase:
    """回声洞数据库管理类"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """初始化数据库表结构"""
        try:
            with self._get_conn() as conn:
                c = conn.cursor()
                # 主表：使用 AUTOINCREMENT 确保 ID 唯一且递增
                c.execute("""
                    CREATE TABLE IF NOT EXISTS cave (
                        cave_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        text TEXT NOT NULL,
                        sender_id INTEGER NOT NULL,
                        group_id INTEGER NOT NULL,
                        group_nick TEXT NOT NULL,
                        pick_count INTEGER DEFAULT 0,
                        date INTEGER NOT NULL,
                        is_deleted INTEGER DEFAULT 0
                    )
                """)
                # 创建索引提升查询性能
                c.execute("CREATE INDEX IF NOT EXISTS idx_sender ON cave(sender_id)")
                c.execute("CREATE INDEX IF NOT EXISTS idx_deleted ON cave(is_deleted)")
                conn.commit()
                logger.info("回声洞数据库初始化完成")
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    @contextmanager
    def _get_conn(self):
        """获取数据库连接的上下文管理器"""
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        try:
            yield conn
        finally:
            conn.close()
    
    def add_cave(self, sender_id: int, group_id: int, group_nick: str, content: str) -> Optional[int]:
        """添加回声洞记录"""
        try:
            timestamp = int(time.time())
            with self._get_conn() as conn:
                c = conn.cursor()
                c.execute(
                    "INSERT INTO cave (text, sender_id, group_id, group_nick, pick_count, date, is_deleted) "
                    "VALUES (?, ?, ?, ?, 0, ?, 0)",
                    (content, sender_id, group_id, group_nick, timestamp)
                )
                conn.commit()
                return c.lastrowid
        except Exception as e:
            logger.error(f"添加回声洞失败: {e}")
            return None
    
    def get_cave(self, cave_id: int) -> Optional[Tuple]:
        """获取指定 ID 的回声洞"""
        try:
            with self._get_conn() as conn:
                c = conn.cursor()
                c.execute(
                    "SELECT cave_id, text, sender_id, group_id, group_nick, pick_count, date, is_deleted "
                    "FROM cave WHERE cave_id = ?",
                    (cave_id,)
                )
                return c.fetchone()
        except Exception as e:
            logger.error(f"查询回声洞失败: {e}")
            return None
    
    def increment_pick_count(self, cave_id: int) -> bool:
        """增加回声洞查看次数"""
        try:
            with self._get_conn() as conn:
                c = conn.cursor()
                c.execute("UPDATE cave SET pick_count = pick_count + 1 WHERE cave_id = ?", (cave_id,))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"更新查看次数失败: {e}")
            return False
    
    def get_random_cave(self) -> Optional[Tuple]:
        """随机获取一条未删除的回声洞"""
        try:
            with self._get_conn() as conn:
                c = conn.cursor()
                c.execute(
                    "SELECT cave_id, text, sender_id, group_id, group_nick, pick_count, date, is_deleted "
                    "FROM cave WHERE is_deleted = 0 ORDER BY RANDOM() LIMIT 1"
                )
                return c.fetchone()
        except Exception as e:
            logger.error(f"随机查询回声洞失败: {e}")
            return None
    
    def get_caves_by_sender(self, sender_id: int, limit: int = 100, offset: int = 0) -> Tuple[List[int], int]:
        """获取指定用户的回声洞列表（分页）"""
        try:
            with self._get_conn() as conn:
                c = conn.cursor()
                # 获取总数
                c.execute("SELECT COUNT(*) FROM cave WHERE sender_id = ? AND is_deleted = 0", (sender_id,))
                total = c.fetchone()[0]
                
                # 获取分页数据
                c.execute(
                    "SELECT cave_id FROM cave WHERE sender_id = ? AND is_deleted = 0 "
                    "ORDER BY cave_id DESC LIMIT ? OFFSET ?",
                    (sender_id, limit, offset)
                )
                rows = c.fetchall()
                return [r[0] for r in rows], total
        except Exception as e:
            logger.error(f"查询用户回声洞失败: {e}")
            return [], 0
    
    def delete_cave(self, cave_id: int) -> bool:
        """软删除回声洞"""
        try:
            with self._get_conn() as conn:
                c = conn.cursor()
                c.execute("UPDATE cave SET is_deleted = 1 WHERE cave_id = ?", (cave_id,))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"删除回声洞失败: {e}")
            return False
    
    def search_caves(self, keyword: str, limit: int = 100) -> List[Tuple]:
        """搜索回声洞（SQL 层限制）"""
        try:
            with self._get_conn() as conn:
                c = conn.cursor()
                c.execute(
                    "SELECT cave_id, text, sender_id, group_id, group_nick, pick_count, date, is_deleted "
                    "FROM cave WHERE is_deleted = 0 AND text LIKE ? "
                    "ORDER BY cave_id DESC LIMIT ?",
                    (f"%{keyword}%", limit)
                )
                return c.fetchall()
        except Exception as e:
            logger.error(f"搜索回声洞失败: {e}")
            return []
    
    def get_max_cave_id(self) -> int:
        """获取当前最大的 cave_id"""
        try:
            with self._get_conn() as conn:
                c = conn.cursor()
                c.execute("SELECT MAX(cave_id) FROM cave")
                result = c.fetchone()[0]
                return result if result else 0
        except Exception as e:
            logger.error(f"获取最大 ID 失败: {e}")
            return 0


async def _get_group_name(event: AstrMessageEvent, group_id: int) -> str:
    """获取群名称"""
    if event.get_platform_name() == "aiocqhttp":
        try:
            from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
            if isinstance(event, AiocqhttpMessageEvent):
                client = event.bot
                payloads = {"group_id": int(group_id)}
                ret = await client.api.call_action('get_group_detail_info', **payloads)
                if ret and isinstance(ret, dict) and 'groupName' in ret:
                    return ret['groupName']
        except Exception as e:
            logger.error(f"获取群名称失败: {e}")
    return "未知群聊"


@register("astrbot_plugin_cave", "lingyu", "基于SQLite3的简单回声洞插件", "0.3")
class CavePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config if config else AstrBotConfig({})
        
        # 初始化数据库路径
        StarTools.get_data_dir()
        if not os.path.exists(plugin_data_dir):
            os.makedirs(plugin_data_dir)
        
        db_path = os.path.join(plugin_data_dir, "cave.db")
        
        # 初始化数据库
        self.db = CaveDatabase(db_path)
        
        # 读取配置
        self.super_admins = self.config.get("super_admins", [])
        self.max_content_length = self.config.get("max_content_length", 200)
        self.page_size = self.config.get("page_size", 100)
        self.quotes = self.config.get("quotes", [])
        self.messages = self.config.get("messages", {})
        
        logger.info(f"回声洞插件已加载，超级管理员: {self.super_admins}")
    
    def _get_message(self, key: str, **kwargs) -> str:
        """获取配置的消息文本"""
        msg = self.messages.get(key, "")
        if kwargs:
            try:
                msg = msg.format(**kwargs)
            except Exception:
                pass
        return msg
    
    def _is_super_admin(self, qq: int) -> bool:
        """检查是否为超级管理员"""
        return qq in self.super_admins
    
    @filter.command("ca")
    async def cave_add(self, event: AstrMessageEvent):
        """添加回声洞"""
        msg = event.message_str.strip()
        parts = msg.split(None, 1)
        content = parts[1].strip() if len(parts) > 1 else ""
        
        if not content:
            yield event.plain_result(self._get_message("empty_content"))
            return
        
        if content.isdigit() or (content.startswith("-") and len(content) > 1 and content[1:].isdigit()):
            yield event.plain_result(self._get_message("number_only"))
            return
        
        # 检查内容长度
        if len(content) > self.max_content_length:
            yield event.plain_result(
                self._get_message("content_too_long", max_length=self.max_content_length)
            )
            return
        
        sender_id = int(event.get_sender_id())
        group_id = int(event.message_obj.group_id) if event.message_obj.group_id else 0
        group_nick = await _get_group_name(event, group_id) if group_id else "私聊"
        
        new_id = self.db.add_cave(sender_id, group_id, group_nick, content)
        
        if new_id:
            quote = random.choice(self.quotes) if self.quotes else ""
            yield event.plain_result(
                self._get_message("add_success", cave_id=new_id, quote=quote)
            )
        else:
            yield event.plain_result(self._get_message("add_failed"))
    
    @filter.command("ci")
    async def cave_inspect(self, event: AstrMessageEvent, cave_id_str: str = ""):
        """查看指定回声洞"""
        cave_id_str = cave_id_str.strip()
        
        if not cave_id_str.isdigit() or int(cave_id_str) <= 0:
            yield event.plain_result(
                f"回声洞 #{cave_id_str}\n\n{self._get_message('invalid_cave')}"
            )
            return
        
        cave_id = int(cave_id_str)
        row = self.db.get_cave(cave_id)
        
        if row is None:
            yield event.plain_result(
                f"回声洞 #{cave_id_str}\n\n{self._get_message('invalid_cave')}"
            )
            return
        
        # row: (cave_id, text, sender_id, group_id, group_nick, pick_count, date, is_deleted)
        if row[7] == 1:  # is_deleted
            yield event.plain_result(
                f"回声洞 #{cave_id}\n\n{self._get_message('deleted_cave')}"
            )
            return
        
        self.db.increment_pick_count(cave_id)
        yield event.plain_result(
            self._get_message(
                "cave_detail",
                cave_id=row[0],
                text=row[1],
                group_nick=row[4],
                pick_count=row[5] + 1
            )
        )
    
    @filter.command("cq")
    async def cave_random(self, event: AstrMessageEvent):
        """随机查看回声洞"""
        row = self.db.get_random_cave()
        
        if row is None:
            yield event.plain_result(self._get_message("cave_empty"))
            return
        
        self.db.increment_pick_count(row[0])
        yield event.plain_result(
            self._get_message(
                "cave_detail",
                cave_id=row[0],
                text=row[1],
                group_nick=row[4],
                pick_count=row[5] + 1
            )
        )
    
    @filter.command("mycave")
    async def my_cave(self, event: AstrMessageEvent):
        """查看自己或指定QQ的回声洞列表（支持分页）
        
        使用方式：
        1. mycave -> 查看自己的第1页
        2. mycave 3 -> 查看自己的第3页
        3. mycave 123456789 -> 查看指定QQ的第1页
        4. mycave 123456789 2 -> 查看指定QQ的第2页
        """
        sender_id = int(event.get_sender_id())
        target_qq = sender_id
        page = 1
        
        # 从消息中解析参数
        msg = event.message_str.strip()
        parts = msg.split()
        args = parts[1:] if len(parts) > 1 else []  # 跳过命令本身
        
        if len(args) == 0:
            # 模式1：不输入参数 -> 默认自己，第1页
            target_qq = sender_id
            page = 1
            
        elif len(args) == 1:
            arg = args[0].strip()
            if not arg.isdigit():
                yield event.plain_result(self._get_message("invalid_number"))
                return
            
            num = int(arg)
            
            if num < 10000:
                # 模式2：一个参数且小于10000 -> 默认自己，参数作为页码
                target_qq = sender_id
                page = num
            else:
                # 模式3：一个参数且大于等于10000 -> 参数作为QQ号，第1页
                target_qq = num
                page = 1
                
        elif len(args) >= 2:
            # 模式4：两个参数 -> 第一个是QQ号，第二个是页码
            arg1 = args[0].strip()
            arg2 = args[1].strip()
            
            if not arg1.isdigit() or not arg2.isdigit():
                yield event.plain_result(self._get_message("invalid_number"))
                return
            
            target_qq = int(arg1)
            page = int(arg2)
        
        # 验证页码有效性
        if page < 1:
            yield event.plain_result(self._get_message("page_must_positive"))
            return
        
        # 计算偏移量
        offset = (page - 1) * self.page_size
        ids, total = self.db.get_caves_by_sender(target_qq, self.page_size, offset)
        
        # 检查是否有数据
        if total == 0:
            yield event.plain_result(self._get_message("no_cave_records", qq=target_qq))
            return
        
        # 计算总页数
        total_pages = (total + self.page_size - 1) // self.page_size
        
        # 检查页码是否越界
        if page > total_pages:
            yield event.plain_result(
                self._get_message("page_out_of_range", qq=target_qq, total_pages=total_pages)
            )
            return
        
        # 构建结果
        if not ids:
            yield event.plain_result(self._get_message("page_no_data", page=page))
            return
        
        id_list = ", ".join(f"#{i}" for i in ids)
        yield event.plain_result(
            self._get_message(
                "mycave_result",
                qq=target_qq,
                page=page,
                total_pages=total_pages,
                total=total,
                id_list=id_list
            )
        )

    
    @filter.command("rmcave")
    async def remove_cave(self, event: AstrMessageEvent, cave_id_str: str = ""):
        """删除指定编号的回声洞"""
        cave_id_str = cave_id_str.strip()
        
        if not cave_id_str.isdigit() or int(cave_id_str) <= 0:
            yield event.plain_result(self._get_message("invalid_cave_id"))
            return
        
        cave_id = int(cave_id_str)
        row = self.db.get_cave(cave_id)
        
        if row is None:
            yield event.plain_result(self._get_message("cave_not_exist", cave_id=cave_id))
            return
        
        # row: (cave_id, text, sender_id, group_id, group_nick, pick_count, date, is_deleted)
        if row[7] == 1:  # is_deleted
            yield event.plain_result(self._get_message("cave_already_deleted", cave_id=cave_id))
            return
        
        sender_id = int(event.get_sender_id())
        cave_sender = row[2]
        
        # 鉴权：只有超级管理员或回声洞创建者可以删除
        if not self._is_super_admin(sender_id) and sender_id != cave_sender:
            yield event.plain_result(self._get_message("no_permission"))
            return
        
        if self.db.delete_cave(cave_id):
            yield event.plain_result(self._get_message("delete_success", cave_id=cave_id))
        else:
            yield event.plain_result(self._get_message("delete_failed"))
    
    @filter.command("cf")
    async def cave_find(self, event: AstrMessageEvent):
        """搜索回声洞"""
        msg = event.message_str.strip()
        parts = msg.split(None, 1)
        keyword = parts[1].strip() if len(parts) > 1 else ""
        
        if not keyword:
            yield event.plain_result(self._get_message("search_empty_keyword"))
            return
        
        # SQL 层直接限制结果数量
        results = self.db.search_caves(keyword, limit=self.page_size)
        
        if not results:
            yield event.plain_result(self._get_message("search_no_result"))
            return
        
        chain = MessageChain().message(
            self._get_message("search_result_header", count=len(results))
        )
        await self.context.send_message(event.unified_msg_origin, chain)
        
        # 分批发送，每批最多 30 条
        batch_size = 30
        
        for i in range(0, len(results), batch_size):
            batch = results[i:i + batch_size]
            nodes = Nodes([])
            
            for r in batch:
                # r: (cave_id, text, sender_id, group_id, group_nick, pick_count, date, is_deleted)
                nodes.nodes.append(
                    Node(
                        uin=event.get_self_id(),
                        name="回声洞",
                        content=[Plain(
                            self._get_message(
                                "search_result_detail",
                                cave_id=r[0],
                                text=r[1],
                                group_nick=r[4],
                                pick_count=r[5]
                            )
                        )]
                    )
                )
            
            try:
                yield event.chain_result([nodes])
            except Exception as e:
                logger.error(f"发送合并消息失败: {e}")
                # 降级：逐条发送
                for r in batch:
                    try:
                        yield event.plain_result(
                            self._get_message(
                                "search_result_detail",
                                cave_id=r[0],
                                text=r[1],
                                group_nick=r[4],
                                pick_count=r[5]
                            )
                        )
                        await asyncio.sleep(0.3)
                    except Exception as send_err:
                        logger.error(f"发送单条消息失败: {send_err}")
            
            # 批次间延迟
            if i + batch_size < len(results):
                await asyncio.sleep(0.5)
    
    async def terminate(self):
        """插件卸载时的清理工作"""
        pass



import sqlite3
import random
import os
import time
from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import logger, AstrBotConfig
from astrbot.api.message_components import Node, Nodes, Plain, Video, Image

# 注册存储路径(数据库格式：SQLite，不可更换，除非有人头铁帮我改，那他就是重音铁头【划掉】)
_DB_DIR = os.path.join("data", "plugin_data", "astrbot_plugin_cave")
_DB_PATH = os.path.join(_DB_DIR, "cave.db")


# 初始化数据目录
def _ensure_db():
    if not os.path.exists(_DB_DIR):
        os.makedirs(_DB_DIR)
    conn = sqlite3.connect(_DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS cave (
            cave_id INTEGER PRIMARY KEY,
            text TEXT,
            sender_id INTEGER,
            group_id INTEGER,
            group_nick TEXT,
            pick_count INTEGER DEFAULT 0,
            date INTEGER
        )
    """)
    c.execute("SELECT cave_id FROM cave WHERE cave_id = 0")
    if c.fetchone() is None:
        c.execute("INSERT INTO cave (cave_id, text, sender_id, group_id, group_nick, pick_count, date) VALUES (0, '0', 0, 0, '', 0, 0)")
    conn.commit()
    conn.close()


# 获取连接(纯看着好看，堪比宏定义)
def _get_conn():
    return sqlite3.connect(_DB_PATH)


# 寻找回声洞最后一条记录(最后一条记录的索引是第0条记录的值)
def _get_max_id():
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT text FROM cave WHERE cave_id = 0")
    row = c.fetchone()
    conn.close()
    return int(row[0]) if row else 0


# 向数据库添加一条记录
def _add_cave(sender_id, group_id, group_nick, content):
    max_id = _get_max_id()
    new_id = max_id + 1
    timestamp = int(time.time())
    conn = _get_conn()
    c = conn.cursor()
    c.execute(
        "INSERT INTO cave (cave_id, text, sender_id, group_id, group_nick, pick_count, date) VALUES (?, ?, ?, ?, ?, 0, ?)",
        (new_id, content, sender_id, group_id, group_nick, timestamp)
    )
    c.execute("UPDATE cave SET text = ? WHERE cave_id = 0", (str(new_id),))
    conn.commit()
    conn.close()
    return new_id


# 在数据库中查找记录
def _get_cave(cave_id):
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT cave_id, text, sender_id, group_id, group_nick, pick_count, date FROM cave WHERE cave_id = ?", (cave_id,))
    row = c.fetchone()
    conn.close()
    return row


# 修改回声洞被阅读次数
def _increment_pick_count(cave_id):
    conn = _get_conn()
    c = conn.cursor()
    c.execute("UPDATE cave SET pick_count = pick_count + 1 WHERE cave_id = ?", (cave_id,))
    conn.commit()
    conn.close()


# 随机获取回声洞的依赖
def _get_random_cave():
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT cave_id, text, sender_id, group_id, group_nick, pick_count, date FROM cave WHERE cave_id > 0 AND text != '此回声洞已被删除' ORDER BY RANDOM() LIMIT 1")
    row = c.fetchone()
    conn.close()
    return row


# 使用《高效》的遍历算法获取账号所属的所有回声洞
def _get_caves_by_sender(sender_id):
    conn = _get_conn()
    c = conn.cursor()
    c.execute("SELECT cave_id FROM cave WHERE sender_id = ? AND cave_id > 0", (sender_id,))
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]


# 删除回声洞
def _delete_cave(cave_id):
    conn = _get_conn()
    c = conn.cursor()
    c.execute("UPDATE cave SET text = '此回声洞已被删除' WHERE cave_id = ?", (cave_id,))
    conn.commit()
    conn.close()


# 搜索回声洞（也是遍历）
def _search_caves(keyword):
    conn = _get_conn()
    c = conn.cursor()
    c.execute(
        "SELECT cave_id, text, sender_id, group_id, group_nick, pick_count, date FROM cave WHERE cave_id > 0 AND text != '此回声洞已被删除' AND text LIKE ?",
        ("%" + keyword + "%",)
    )
    rows = c.fetchall()
    conn.close()
    return rows


# 从群号获取群名称
async def _get_group_name(event: AstrMessageEvent, group_id: int):
    """获取群名称"""
    if event.get_platform_name() == "aiocqhttp":
        try:
            from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
            if isinstance(event, AiocqhttpMessageEvent):
                client = event.bot
                payloads = {"group_id": int(group_id)}
                ret = await client.api.call_action('get_group_detail_info', **payloads)
                logger.info(f"获取群信息返回: {ret}")
                if ret and isinstance(ret, dict) and 'groupName' in ret:
                    return ret['groupName']
        except Exception as e:
            logger.error(f"获取群名称失败: {e}")
    return "未知群聊"


# 此程序仅在aiocqhttp环境下测试通过，且大量依赖NapCat接口，仅支持NapCat&AstrBot4.9+
# 测试环境：AstrBot4.15 + NapCat个人号
# 作者仅提供软件本体，使用者自行为回声洞中所有信息负责


@register("astrbot_plugin_cave", "lingyu", "基于SQLite3的简单回声洞插件", "1.0.0", "")
class CavePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        _ensure_db()
        
        # 读取配置文件
        self.super_admins = self.config.get("super_admins", [])
        self.forward_target = self.config.get("forward_target", )
        self.quotes = self.config.get("quotes", [])
        
        # 发送日志确认加载成功
        logger.info(f"回声洞插件已加载，超级管理员: {self.super_admins}")

    # 经典写个函数功能性堪比宏定义
    def _is_super_admin(self, qq: int) -> bool:
        return qq in self.super_admins

    # 添加回声洞命令（这里的很多结构都是给作者自己的bot用的，其他bot可能需要改动）
    @filter.command("ca")
    async def cave_add(self, event: AstrMessageEvent):
        msg = event.message_str.strip()
        parts = msg.split(None, 1)
        content = parts[1].strip() if len(parts) > 1 else ""

        if not content:
            yield event.plain_result("唔...回声洞不可以上传滚木哦")
            return

        if content.isdigit() or (content.startswith("-") and len(content) > 1 and content[1:].isdigit()):
            yield event.plain_result("只输入了数字...猜你想用的是ci吧")
            return

        sender_id = int(event.get_sender_id())
        group_id = int(event.message_obj.group_id) if event.message_obj.group_id else 0
        
        group_nick = await _get_group_name(event, group_id) if group_id else "私聊"
        
        new_id = _add_cave(sender_id, group_id, group_nick, content)
        quote = random.choice(self.quotes) if self.quotes else ""
        yield event.plain_result(f"回声洞 #{new_id} 添加成功！\n\n{quote}")

    # 查看回声洞命令（不合法返回滚木可以改成报错）
    @filter.command("ci")
    async def cave_inspect(self, event: AstrMessageEvent, cave_id_str: str = ""):
        cave_id_str = cave_id_str.strip()

        valid = False
        if cave_id_str.isdigit():
            cave_id = int(cave_id_str)
            if 0 < cave_id <= _get_max_id():
                valid = True

        if not valid:
            yield event.plain_result(f"回声洞 #{cave_id_str}\n\n滚木滚木滚木")
            return

        row = _get_cave(int(cave_id_str))
        if row is None:
            yield event.plain_result(f"回声洞 #{cave_id_str}\n\n滚木滚木滚木")
            return

        _increment_pick_count(int(cave_id_str))
        yield event.plain_result(f"回声洞 #{row[0]}\n\n{row[1]}\n\n——发布自：{row[4]}\n此回声洞已被查看{row[5] + 1}次")

    # 随机查看回声洞命令
    @filter.command("cq")
    async def cave_random(self, event: AstrMessageEvent):
        row = _get_random_cave()
        if row is None:
            yield event.plain_result("回声洞里空空如也...")
            return
        _increment_pick_count(row[0])
        yield event.plain_result(f"回声洞 #{row[0]}\n\n{row[1]}\n\n——发布自：{row[4]}\n此回声洞已被查看{row[5] + 1}次")

    # 查看自己或指定QQ的回声洞编号列表
    @filter.command("mycave")
    async def my_cave(self, event: AstrMessageEvent, target_qq: str = ""):
        target_qq = target_qq.strip()

        if target_qq:
            if not target_qq.isdigit():
                yield event.plain_result("请输入有效的QQ号")
                return
            sender = int(target_qq)
        else:
            sender = int(event.get_sender_id())

        ids = _get_caves_by_sender(sender)
        if not ids:
            yield event.plain_result("没有找到相关的回声洞记录")
            return

        id_list = ", ".join(f"#{i}" for i in ids)
        yield event.plain_result(f"QQ {sender} 的回声洞编号：\n{id_list}")

    # 删除回声洞命令（此处有鉴权）
    @filter.command("rmcave")
    async def remove_cave(self, event: AstrMessageEvent, cave_id_str: str = ""):
        '''删除指定编号的回声洞'''
        cave_id_str = cave_id_str.strip()
        if not cave_id_str.isdigit() or int(cave_id_str) <= 0:
            yield event.plain_result("请输入有效的回声洞编号")
            return

        cave_id = int(cave_id_str)
        row = _get_cave(cave_id)
        if row is None:
            yield event.plain_result(f"回声洞 #{cave_id} 不存在")
            return

        if row[1] == "此回声洞已被删除":
            yield event.plain_result(f"回声洞 #{cave_id} 已经被删除过了")
            return

        sender_id = int(event.get_sender_id())
        cave_sender = row[2]

        # 鉴权：只有超级管理员或回声洞创建者可以删除
        if not self._is_super_admin(sender_id) and sender_id != cave_sender:
            yield event.plain_result("你没有权限删除这条回声洞")
            return

        _delete_cave(cave_id)
        yield event.plain_result(f"回声洞 #{cave_id} 已删除")

    # 搜索回声洞（关键词）
    @filter.command("cf")
    async def cave_find(self, event: AstrMessageEvent):
        msg = event.message_str.strip()
        parts = msg.split(None, 1)
        keyword = parts[1].strip() if len(parts) > 1 else ""

        if not keyword:
            yield event.plain_result("你输入了滚木，我在洞里给你找来了！看，滚木回声洞！")
            return

        results = _search_caves(keyword)
        if not results:
            yield event.plain_result("唔...翻了一圈也没找到呢")
            return

        results = results[:100]

        for r in results:
            _increment_pick_count(r[0])

        chain = MessageChain().message(f"找到啦！一共有整整 {len(results)} 条呐！")
        await self.context.send_message(event.unified_msg_origin, chain)

        import asyncio
        batch_size = 30
        batch_count = (len(results) + batch_size - 1) // batch_size
        
        for i in range(0, len(results), batch_size):
            batch = results[i:i + batch_size]
            nodes = Nodes([])
            for r in batch:
                nodes.nodes.append(
                    Node(
                        uin = event.get_self_id(),
                        name = "回声洞",
                        content=[Plain(f"回声洞 #{r[0]}\n\n{r[1]}\n\n——发布自：{r[4]}\n此回声洞已被查看{r[5] + 1}次")]
                    )
                )
            
            yield event.chain_result([nodes])
        
        # 如果不是最后一批，等待一下避免发送过快
            if i + batch_size < len(results):
                await asyncio.sleep(0.5)


    # 啥也不会发生喵
    async def terminate(self):
        pass

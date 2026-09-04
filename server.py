#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
知识图谱工作台后端 · SQLite存储版 · 多图谱（按业务拆分）
启动:  python3 server.py
访问:  http://127.0.0.1:8765
数据:  ./knowledge_graph.db (SQLite, WAL模式)
图谱:  graphs 表按业务拆分；nodes/edges/versions/backups 均挂 graph_id
备份:  每次变更前自动快照(按图谱), ≥60秒一条, 保留最近15条
"""
import json
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(ROOT, 'knowledge_graph.db')
SEED_PATH = os.path.join(ROOT, 'seed.json')
# 本地默认 8765；部署沙箱通过 PORT 环境变量注入端口并要求绑 0.0.0.0
PORT = int(os.environ.get('PORT', 8765))
BIND_HOST = '0.0.0.0' if os.environ.get('PORT') else '127.0.0.1'
BACKUP_INTERVAL = 60      # 秒
BACKUP_KEEP = 15          # 保留条数

# 国际供应链·总览 课件清单（按学习手册、0–10 依次排序）
COURSEWARE = [
    ('manual', '📘 学习手册 · 国际物流与跨境供应链', '国际物流与跨境供应链学习手册.md'),
    ('c0', '第0章 · 入门知识与案例', '国际供应链-第0章-入门知识与案例.md'),
    ('c1', '第1章 · 结构与流程', '国际供应链-第1章-结构与流程.md'),
    ('c2', '第2章 · 国际运输方式', '国际供应链-第2章-国际运输方式.md'),
    ('c3', '第3章 · 关务与合规', '国际供应链-第3章-关务与合规.md'),
    ('c4', '第4章 · 仓储与库存', '国际供应链-第4章-仓储与库存.md'),
    ('c5', '第5章 · 单据与单证', '国际供应链-第5章-单据与单证.md'),
    ('c6', '第6章 · 跨境支付与结算', '国际供应链-第6章-跨境支付与结算.md'),
    ('c7', '第7章 · 风险韧性与趋势', '国际供应链-第7章-风险韧性与趋势.md'),
    ('c8', '第8章 · 供应链系统与数字化', '国际供应链-第8章-供应链系统与数字化.md'),
    ('c9', '第9章 · 关键指标与绩效管理', '国际供应链-第9章-关键指标与绩效管理.md'),
    ('c10', '第10章 · 未来趋势与终章', '国际供应链-第10章-未来趋势与终章.md'),
]
CW_MAP = {cid: fn for cid, _, fn in COURSEWARE}

_last_backup = [0.0]

PRESENCE_TTL = 30  # 秒，超过无心跳视为离线
_presence = {}     # clientId -> {name, color, editing, ts}
_presence_lock = threading.Lock()


class ApiError(Exception):
    pass


class ConflictError(Exception):
    """乐观锁冲突：携带服务端当前数据供前端展示"""
    def __init__(self, message, current):
        super().__init__(message)
        self.current = current


def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def now_iso():
    return datetime.now().isoformat(timespec='seconds')


def gen_id(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


SCHEMA = '''
CREATE TABLE IF NOT EXISTS graphs(
  id TEXT PRIMARY KEY, name TEXT NOT NULL, descr TEXT DEFAULT '', created_at TEXT,
  deleted_at TEXT
);
CREATE TABLE IF NOT EXISTS nodes(
  id TEXT PRIMARY KEY, graph_id TEXT, label TEXT NOT NULL, category TEXT NOT NULL,
  descr TEXT DEFAULT '', tags TEXT DEFAULT '[]',
  created_at TEXT, updated_at TEXT, deleted_at TEXT
);
CREATE TABLE IF NOT EXISTS edges(
  id TEXT PRIMARY KEY, graph_id TEXT, source TEXT NOT NULL, target TEXT NOT NULL,
  relation TEXT NOT NULL, weight INTEGER DEFAULT 2, descr TEXT DEFAULT '',
  created_at TEXT, updated_at TEXT, deleted_at TEXT
);
CREATE TABLE IF NOT EXISTS versions(
  id TEXT PRIMARY KEY, graph_id TEXT, name TEXT NOT NULL, descr TEXT DEFAULT '',
  effective_from TEXT, effective_to TEXT, ts TEXT,
  nodes_json TEXT, edges_json TEXT, deleted_at TEXT
);
CREATE TABLE IF NOT EXISTS backups(
  id TEXT PRIMARY KEY, graph_id TEXT, ts TEXT, nodes_json TEXT, edges_json TEXT,
  deleted_at TEXT
);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
'''


def migrate_graphs(conn):
    """旧库迁移：补 graph_id 列 + deleted_at 列（逻辑删除），建默认图谱并挂存量数据"""
    for table in ('nodes', 'edges', 'versions', 'backups'):
        cols = {r[1] for r in conn.execute(f'PRAGMA table_info({table})')}
        if 'graph_id' not in cols:
            conn.execute(f'ALTER TABLE {table} ADD COLUMN graph_id TEXT')
    for table in ('graphs', 'nodes', 'edges', 'versions', 'backups'):
        cols = {r[1] for r in conn.execute(f'PRAGMA table_info({table})')}
        if 'deleted_at' not in cols:
            conn.execute(f'ALTER TABLE {table} ADD COLUMN deleted_at TEXT')
    if conn.execute('SELECT COUNT(*) c FROM graphs').fetchone()['c'] == 0:
        gid = gen_id('g')
        conn.execute('INSERT INTO graphs(id,name,descr,created_at) VALUES(?,?,?,?)',
                     (gid, '国际物流 · 跨境供应链', '默认图谱（迁移自单图谱数据）', now_iso()))
        for table in ('nodes', 'edges', 'versions', 'backups'):
            conn.execute(f'UPDATE {table} SET graph_id=? WHERE graph_id IS NULL', (gid,))
        conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('seed_graph_id',?)", (gid,))


def init_db():
    conn = db()
    conn.executescript(SCHEMA)
    migrate_graphs(conn)
    # 索引须在迁移补列之后创建
    conn.execute('CREATE INDEX IF NOT EXISTS idx_nodes_g ON nodes(graph_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_edges_g ON edges(graph_id)')
    conn.commit()
    if conn.execute('SELECT COUNT(*) c FROM nodes').fetchone()['c'] == 0:
        r = conn.execute('SELECT id FROM graphs ORDER BY created_at LIMIT 1').fetchone()
        gid = r['id'] if r else create_graph(conn, '默认图谱')
        seed_db(conn, gid)
    conn.commit()
    conn.close()


def create_graph(conn, name, descr=''):
    gid = gen_id('g')
    conn.execute('INSERT INTO graphs(id,name,descr,created_at) VALUES(?,?,?,?)',
                 (gid, name, descr, now_iso()))
    return gid


def seed_db(conn, gid):
    """用 seed.json 初始化指定图谱的节点与关联"""
    with open(SEED_PATH, encoding='utf-8') as f:
        seed = json.load(f)
    ts = now_iso()
    for n in seed['nodes']:
        conn.execute(
            'INSERT OR REPLACE INTO nodes(id,graph_id,label,category,descr,tags,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)',
            (n['id'], gid, n['label'], n['category'], n.get('desc', ''),
             json.dumps(n.get('tags', []), ensure_ascii=False), ts, ts))
    for e in seed['edges']:
        conn.execute(
            'INSERT OR REPLACE INTO edges(id,graph_id,source,target,relation,weight,descr,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)',
            (gen_id('e'), gid, e['source'], e['target'], e['relation'],
             e.get('weight', 2), e.get('desc', ''), ts, ts))
    conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('seed_graph_id',?)", (gid,))


# ---------- 行转换 ----------
def node_row(r):
    return {'id': r['id'], 'graphId': r['graph_id'], 'label': r['label'], 'category': r['category'],
            'desc': r['descr'], 'tags': json.loads(r['tags'] or '[]'),
            'createdAt': r['created_at'], 'updatedAt': r['updated_at'],
            'deletedAt': r['deleted_at'] if 'deleted_at' in r.keys() else None}


def edge_row(r):
    return {'id': r['id'], 'graphId': r['graph_id'], 'source': r['source'], 'target': r['target'],
            'relation': r['relation'], 'weight': r['weight'], 'desc': r['descr'],
            'createdAt': r['created_at'], 'updatedAt': r['updated_at'],
            'deletedAt': r['deleted_at'] if 'deleted_at' in r.keys() else None}


def version_row(r, with_data=True):
    v = {'id': r['id'], 'graphId': r['graph_id'], 'name': r['name'], 'desc': r['descr'],
         'effectiveFrom': r['effective_from'], 'effectiveTo': r['effective_to'], 'timestamp': r['ts'],
         'deletedAt': r['deleted_at'] if 'deleted_at' in r.keys() else None}
    if with_data:
        v['nodes'] = json.loads(r['nodes_json'])
        v['edges'] = json.loads(r['edges_json'])
    return v


def backup_row(r, with_data=True):
    b = {'id': r['id'], 'graphId': r['graph_id'], 'timestamp': r['ts'],
         'deletedAt': r['deleted_at'] if 'deleted_at' in r.keys() else None}
    if with_data:
        b['nodes'] = json.loads(r['nodes_json'])
        b['edges'] = json.loads(r['edges_json'])
    return b


def dump_nodes(conn, gid):
    return [node_row(r) for r in conn.execute(
        'SELECT * FROM nodes WHERE graph_id=? AND deleted_at IS NULL ORDER BY label', (gid,))]


def dump_edges(conn, gid):
    return [edge_row(r) for r in conn.execute(
        'SELECT * FROM edges WHERE graph_id=? AND deleted_at IS NULL', (gid,))]


def list_graphs(conn):
    graphs = []
    for g in conn.execute('SELECT * FROM graphs WHERE deleted_at IS NULL ORDER BY created_at'):
        nc = conn.execute('SELECT COUNT(*) c FROM nodes WHERE graph_id=? AND deleted_at IS NULL',
                          (g['id'],)).fetchone()['c']
        ec = conn.execute('SELECT COUNT(*) c FROM edges WHERE graph_id=? AND deleted_at IS NULL',
                          (g['id'],)).fetchone()['c']
        graphs.append({'id': g['id'], 'name': g['name'], 'descr': g['descr'],
                       'createdAt': g['created_at'], 'nodeCount': nc, 'edgeCount': ec})
    return graphs


def require_graph(conn, gid):
    if not gid:
        raise ApiError('缺少图谱标识(graph)')
    r = conn.execute('SELECT id FROM graphs WHERE id=? AND deleted_at IS NULL', (gid,)).fetchone()
    if not r:
        raise ApiError('图谱不存在或已被删除')
    return gid


def get_current_version(conn):
    r = conn.execute("SELECT value FROM meta WHERE key='current_version'").fetchone()
    return r['value'] if r else '初始数据'


def bump_version(conn):
    """全局数据版本号 +1（同事务提交，供客户端轮询变更）"""
    conn.execute(
        "INSERT INTO meta(key,value) VALUES('data_version','1') "
        "ON CONFLICT(key) DO UPDATE SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT)")


def get_data_version(conn):
    r = conn.execute("SELECT value FROM meta WHERE key='data_version'").fetchone()
    try:
        return int(r['value'])
    except (TypeError, ValueError):
        return 0


def get_state(conn, gid):
    versions = [version_row(r) for r in conn.execute(
        'SELECT * FROM versions WHERE graph_id=? AND deleted_at IS NULL ORDER BY ts DESC', (gid,))]
    backups = [backup_row(r) for r in conn.execute(
        'SELECT * FROM backups WHERE graph_id=? AND deleted_at IS NULL ORDER BY ts DESC', (gid,))]
    return {'graphs': list_graphs(conn),
            'currentGraph': gid,
            'nodes': dump_nodes(conn, gid), 'edges': dump_edges(conn, gid),
            'versions': versions, 'backups': backups,
            'currentVersion': get_current_version(conn),
            'dataVersion': get_data_version(conn)}


def snapshot_json(conn, gid):
    return (json.dumps(dump_nodes(conn, gid), ensure_ascii=False),
            json.dumps(dump_edges(conn, gid), ensure_ascii=False))


def maybe_backup(conn, gid):
    """变更前按图谱快照当前数据；距上次≥60秒才记一条；保留最近15条"""
    if time.time() - _last_backup[0] < BACKUP_INTERVAL:
        return
    nj, ej = snapshot_json(conn, gid)
    conn.execute('INSERT INTO backups(id,graph_id,ts,nodes_json,edges_json) VALUES(?,?,?,?,?)',
                 (gen_id('b'), gid, now_iso(), nj, ej))
    old = [r['id'] for r in conn.execute(
        'SELECT id FROM backups WHERE graph_id=? ORDER BY ts DESC', (gid,))]
    for oid in old[BACKUP_KEEP:]:
        conn.execute('DELETE FROM backups WHERE id=?', (oid,))
    _last_backup[0] = time.time()


def replace_graph(conn, gid, nodes, edges):
    """恢复/替换：把该图谱当前存活数据整体移入回收站，再按快照内容复活/重建"""
    ts = now_iso()
    conn.execute('UPDATE edges SET deleted_at=?, updated_at=? WHERE graph_id=? AND deleted_at IS NULL',
                 (ts, ts, gid))
    conn.execute('UPDATE nodes SET deleted_at=?, updated_at=? WHERE graph_id=? AND deleted_at IS NULL',
                 (ts, ts, gid))
    for n in nodes:
        conn.execute(
            'INSERT OR REPLACE INTO nodes(id,graph_id,label,category,descr,tags,created_at,updated_at,deleted_at) VALUES(?,?,?,?,?,?,?,?,NULL)',
            (n['id'], gid, n.get('label', '未命名'), n.get('category', '概念'),
             n.get('desc', ''), json.dumps(n.get('tags', []), ensure_ascii=False),
             n.get('createdAt', ts), ts))
    for e in edges:
        conn.execute(
            'INSERT OR REPLACE INTO edges(id,graph_id,source,target,relation,weight,descr,created_at,updated_at,deleted_at) VALUES(?,?,?,?,?,?,?,?,?,NULL)',
            (e['id'], gid, e['source'], e['target'], e['relation'], e.get('weight', 2),
             e.get('desc', ''), e.get('createdAt', ts), ts))


def soft_delete_node(conn, nid, gid, ts):
    """逻辑删除节点，并级联逻辑删除其关联（同一时间戳，便于一键恢复）"""
    conn.execute(
        'UPDATE edges SET deleted_at=?, updated_at=? WHERE graph_id=? AND deleted_at IS NULL AND (source=? OR target=?)',
        (ts, ts, gid, nid, nid))
    conn.execute('UPDATE nodes SET deleted_at=?, updated_at=? WHERE id=? AND graph_id=?',
                 (ts, ts, nid, gid))


def soft_delete_graph(conn, ggid, ts):
    """逻辑删除图谱及其全部内容（含版本历史与自动备份，恢复图谱时一并还原）"""
    conn.execute('UPDATE edges SET deleted_at=?, updated_at=? WHERE graph_id=? AND deleted_at IS NULL',
                 (ts, ts, ggid))
    conn.execute('UPDATE nodes SET deleted_at=?, updated_at=? WHERE graph_id=? AND deleted_at IS NULL',
                 (ts, ts, ggid))
    # versions/backups 表没有 updated_at 列，只标记 deleted_at
    conn.execute('UPDATE versions SET deleted_at=? WHERE graph_id=? AND deleted_at IS NULL', (ts, ggid))
    conn.execute('UPDATE backups SET deleted_at=? WHERE graph_id=? AND deleted_at IS NULL', (ts, ggid))
    conn.execute('UPDATE graphs SET deleted_at=? WHERE id=?', (ts, ggid))


# ---------- 回收站 ----------
def _label_map(conn):
    return {r['id']: r['label'] for r in conn.execute('SELECT id,label FROM nodes')}


def trash_items(conn):
    """全部逻辑删除条目（含完整字段与所属图谱名），按删除时间倒序"""
    gnames = {g['id']: g['name'] for g in conn.execute('SELECT id,name FROM graphs')}
    labels = _label_map(conn)
    items = []
    for r in conn.execute('SELECT * FROM nodes WHERE deleted_at IS NOT NULL ORDER BY deleted_at DESC'):
        items.append({'type': 'node', 'id': r['id'], 'graphId': r['graph_id'],
                      'graphName': gnames.get(r['graph_id'], '未知图谱'),
                      'label': r['label'], 'category': r['category'], 'desc': r['descr'],
                      'tags': json.loads(r['tags'] or '[]'),
                      'createdAt': r['created_at'], 'deletedAt': r['deleted_at'],
                      'summary': f'{r["label"]}（{r["category"]}）'})
    for r in conn.execute('SELECT * FROM edges WHERE deleted_at IS NOT NULL ORDER BY deleted_at DESC'):
        s, t = labels.get(r['source'], r['source']), labels.get(r['target'], r['target'])
        items.append({'type': 'edge', 'id': r['id'], 'graphId': r['graph_id'],
                      'graphName': gnames.get(r['graph_id'], '未知图谱'),
                      'source': r['source'], 'target': r['target'], 'relation': r['relation'],
                      'weight': r['weight'], 'desc': r['descr'],
                      'createdAt': r['created_at'], 'deletedAt': r['deleted_at'],
                      'summary': f'{s} → {t}（{r["relation"]}）'})
    for r in conn.execute('SELECT * FROM versions WHERE deleted_at IS NOT NULL ORDER BY deleted_at DESC'):
        items.append({'type': 'version', 'id': r['id'], 'graphId': r['graph_id'],
                      'graphName': gnames.get(r['graph_id'], '未知图谱'),
                      'name': r['name'], 'desc': r['descr'],
                      'effectiveFrom': r['effective_from'], 'effectiveTo': r['effective_to'],
                      'timestamp': r['ts'],
                      'nodes': json.loads(r['nodes_json'] or '[]'),
                      'edges': json.loads(r['edges_json'] or '[]'),
                      'deletedAt': r['deleted_at'],
                      'summary': f'版本「{r["name"]}」'})
    for r in conn.execute('SELECT * FROM graphs WHERE deleted_at IS NOT NULL ORDER BY deleted_at DESC'):
        nc = conn.execute('SELECT COUNT(*) c FROM nodes WHERE graph_id=?', (r['id'],)).fetchone()['c']
        ec = conn.execute('SELECT COUNT(*) c FROM edges WHERE graph_id=?', (r['id'],)).fetchone()['c']
        items.append({'type': 'graph', 'id': r['id'], 'graphId': r['id'],
                      'graphName': r['name'], 'name': r['name'], 'descr': r['descr'],
                      'createdAt': r['created_at'], 'deletedAt': r['deleted_at'],
                      'summary': f'图谱「{r["name"]}」（含 {nc} 节点 / {ec} 关联及版本历史）'})
    return items


def trash_restore(conn, typ, iid):
    ts_field = {'node': 'nodes', 'edge': 'edges', 'version': 'versions', 'graph': 'graphs'}
    if typ not in ts_field:
        raise ApiError(f'不支持的类型: {typ}')
    table = ts_field[typ]
    r = conn.execute(f'SELECT * FROM {table} WHERE id=? AND deleted_at IS NOT NULL', (iid,)).fetchone()
    if not r:
        raise ApiError('回收站中不存在该条目（可能已被恢复或彻底删除）')
    if typ != 'graph':
        g = conn.execute('SELECT deleted_at FROM graphs WHERE id=?', (r['graph_id'],)).fetchone()
        if not g or g['deleted_at']:
            raise ApiError('所在图谱已被删除，请先从回收站恢复该图谱')
    ts = r['deleted_at']
    if typ == 'node':
        # 恢复节点 + 与其同批级联删除的关联
        conn.execute('UPDATE nodes SET deleted_at=NULL WHERE id=?', (iid,))
        conn.execute('UPDATE edges SET deleted_at=NULL WHERE deleted_at=? AND (source=? OR target=?)',
                     (ts, iid, iid))
        return f'节点「{r["label"]}」及其关联已恢复'
    if typ == 'edge':
        conn.execute('UPDATE edges SET deleted_at=NULL WHERE id=?', (iid,))
        return '关联已恢复'
    if typ == 'version':
        conn.execute('UPDATE versions SET deleted_at=NULL WHERE id=?', (iid,))
        return f'版本「{r["name"]}」已恢复'
    # graph：恢复图谱 + 同批级联删除的全部内容
    conn.execute('UPDATE graphs SET deleted_at=NULL WHERE id=?', (iid,))
    for t in ('edges', 'nodes', 'versions', 'backups'):
        conn.execute(f'UPDATE {t} SET deleted_at=NULL WHERE graph_id=? AND deleted_at=?', (iid, ts))
    return f'图谱「{r["name"]}」及其全部内容已恢复'


def trash_purge(conn, typ, iid):
    tables = {'node': 'nodes', 'edge': 'edges', 'version': 'versions', 'graph': 'graphs'}
    if typ not in tables:
        raise ApiError(f'不支持的类型: {typ}')
    tname = tables[typ]
    r = conn.execute(f'SELECT * FROM {tname} WHERE id=? AND deleted_at IS NOT NULL', (iid,)).fetchone()
    if not r:
        raise ApiError('回收站中不存在该条目')
    if typ == 'graph':
        for t in ('edges', 'nodes', 'versions', 'backups'):
            conn.execute(f'DELETE FROM {t} WHERE graph_id=?', (iid,))
    if typ == 'node':
        # 连带清掉同批级联删除且仍无其他存活端点的关联，避免悬空
        conn.execute(
            'DELETE FROM edges WHERE graph_id=? AND deleted_at IS NOT NULL AND (source=? OR target=?)',
            (r['graph_id'], iid, iid))
    conn.execute(f'DELETE FROM {tname} WHERE id=?', (iid,))
    return True


def validate_window(from_val, to_val):
    if to_val and not from_val:
        raise ApiError('有失效限制必须有生效时间')
    if from_val and to_val and to_val <= from_val:
        raise ApiError('失效时间必须晚于生效时间')


def check_optimistic(row, body, what, current):
    """乐观锁：请求带 baseUpdatedAt 时校验该行未被他人修改"""
    base = body.get('baseUpdatedAt')
    if base and base != row['updated_at']:
        raise ConflictError(f'该{what}已被他人修改，请选择保留哪个版本', current)


# ---------- HTTP Handler ----------
class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=ROOT, **kwargs)

    def log_message(self, fmt, *args):
        pass

    # ---- helpers ----
    def _json(self, obj, status=200, headers=None):
        data = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-store')
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        n = int(self.headers.get('Content-Length') or 0)
        raw = self.rfile.read(n) if n else b'{}'
        try:
            return json.loads(raw.decode('utf-8') or '{}')
        except json.JSONDecodeError:
            raise ApiError('请求体不是合法JSON')

    def _query_graph(self):
        q = parse_qs(urlparse(self.path).query)
        return (q.get('graph') or [None])[0]

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def mutate(self, fn):
        """写操作统一包装：先自动备份 → 执行 → data_version+1 → 提交"""
        conn = db()
        try:
            result = fn(conn) or {'ok': True}
            bump_version(conn)
            conn.commit()
            result['dataVersion'] = get_data_version(conn)
            self._json(result)
        except ApiError as e:
            conn.rollback()
            self._json({'error': str(e)}, 400)
        except ConflictError as e:
            conn.rollback()
            self._json({'error': str(e), 'current': e.current}, 409)
        except Exception as e:
            conn.rollback()
            self._json({'error': f'{type(e).__name__}: {e}'}, 500)
        finally:
            conn.close()

    # ---- GET ----
    def do_GET(self):
        p = urlparse(self.path).path
        if p == '/':
            with open(os.path.join(ROOT, '知识图谱工作台.html'), 'rb') as f:
                data = f.read()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(data)
            return
        if p == '/api/courseware':
            return self._json([{'id': c[0], 'title': c[1], 'file': c[2]} for c in COURSEWARE])
        # 静态关键文件改代码直读（与 / 同模式）：SimpleHTTPRequestHandler 路径经云代理偶发 502
        if p in ('/courseware.html', '/marked.min.js'):
            fn = 'courseware.html' if p == '/courseware.html' else 'marked.min.js'
            fpath = os.path.join(ROOT, fn)
            if not os.path.isfile(fpath):
                return self._json({'error': 'not found'}, 404)
            with open(fpath, 'rb') as f:
                data = f.read()
            ctype = 'text/html; charset=utf-8' if fn.endswith('.html') else 'application/javascript; charset=utf-8'
            self.send_response(200)
            self.send_header('Content-Type', ctype)
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(data)
            return
        m = __import__('re').fullmatch(r'/api/md/([^/]+)', p)
        if m:
            cid = m.group(1)
            fn = CW_MAP.get(cid)
            if not fn:
                return self._json({'error': 'not found'}, 404)
            fpath = os.path.join(ROOT, fn)
            if not os.path.isfile(fpath):
                return self._json({'error': 'file missing'}, 404)
            with open(fpath, 'r', encoding='utf-8') as fp:
                text = fp.read()
            body = text.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/markdown; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(body)
            return
        if p == '/api/state':
            conn = db()
            try:
                gid = self._query_graph() or \
                    conn.execute('SELECT id FROM graphs WHERE deleted_at IS NULL ORDER BY created_at LIMIT 1').fetchone()['id']
                require_graph(conn, gid)
                return self._json(get_state(conn, gid))
            except ApiError as e:
                return self._json({'error': str(e)}, 400)
            finally:
                conn.close()
        if p == '/api/trash':
            conn = db()
            try:
                return self._json({'items': trash_items(conn)})
            finally:
                conn.close()
        if p == '/api/export':
            conn = db()
            try:
                gid = self._query_graph() or \
                    conn.execute('SELECT id FROM graphs ORDER BY created_at LIMIT 1').fetchone()['id']
                payload = {'app': '国际物流知识图谱工作台', 'exportTime': now_iso(),
                           'currentGraph': gid, 'currentGraphId': gid,
                           'currentVersion': get_current_version(conn),
                           'graphs': list_graphs(conn),
                           'nodes': [node_row(r) for r in conn.execute('SELECT * FROM nodes WHERE deleted_at IS NULL')],
                           'edges': [edge_row(r) for r in conn.execute('SELECT * FROM edges WHERE deleted_at IS NULL')],
                           'versions': [version_row(r) for r in conn.execute('SELECT * FROM versions WHERE deleted_at IS NULL')],
                           'backups': [backup_row(r) for r in conn.execute('SELECT * FROM backups WHERE deleted_at IS NULL')],
                           'trash': trash_items(conn)}
                data = json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Disposition', 'attachment; filename="knowledge-graph-export.json"')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            finally:
                conn.close()
            return
        if p.startswith('/api/'):
            return self._json({'error': 'not found'}, 404)
        return super().do_GET()

    # ---- POST ----
    def do_POST(self):
        p = urlparse(self.path).path

        if p == '/api/presence':
            body = self._body()
            cid = body.get('clientId')
            if not cid:
                return self._json({'error': 'clientId 必填'}, 400)
            now = time.time()
            with _presence_lock:
                for k in [k for k, v in _presence.items() if now - v['ts'] > PRESENCE_TTL]:
                    del _presence[k]
                _presence[cid] = {
                    'name': body.get('name') or '匿名',
                    'color': body.get('color') or '#534ab7',
                    'editing': body.get('editing'),
                    'ts': now,
                }
                online = [{'clientId': k, 'name': v['name'],
                           'color': v['color'], 'editing': v['editing']}
                          for k, v in sorted(_presence.items())]
            conn = db()
            try:
                dv = get_data_version(conn)
            finally:
                conn.close()
            return self._json({'online': online, 'dataVersion': dv})

        if p == '/api/graphs':
            body = self._body()
            if not body.get('name'):
                return self._json({'error': '图谱名称必填'}, 400)
            def fn(conn):
                gid = create_graph(conn, body['name'].strip(), body.get('descr', ''))
                return {'id': gid}
            return self.mutate(fn)

        if p == '/api/nodes':
            body = self._body()
            if not body.get('label') or not body.get('category'):
                return self._json({'error': 'label 和 category 必填'}, 400)
            def fn(conn):
                gid = require_graph(conn, body.get('graphId'))
                maybe_backup(conn, gid)
                ts = now_iso()
                nid = body.get('id') or gen_id('n')
                if conn.execute('SELECT 1 FROM nodes WHERE id=?', (nid,)).fetchone():
                    raise ApiError(f'节点id已存在: {nid}')
                conn.execute(
                    'INSERT INTO nodes(id,graph_id,label,category,descr,tags,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)',
                    (nid, gid, body['label'], body['category'], body.get('desc', ''),
                     json.dumps(body.get('tags', []), ensure_ascii=False), ts, ts))
                return {'id': nid}
            return self.mutate(fn)

        if p == '/api/edges':
            body = self._body()
            for k in ('source', 'target', 'relation'):
                if not body.get(k):
                    return self._json({'error': f'{k} 必填'}, 400)
            if body['source'] == body['target']:
                return self._json({'error': '不能关联自身'}, 400)
            def fn(conn):
                gid = require_graph(conn, body.get('graphId'))
                maybe_backup(conn, gid)
                ts = now_iso()
                eid = body.get('id') or gen_id('e')
                r0 = conn.execute('SELECT deleted_at FROM edges WHERE id=?', (eid,)).fetchone()
                if r0:
                    raise ApiError(f'关联id已存在: {eid}' +
                                   ('（在回收站中，可先恢复或彻底删除）' if r0['deleted_at'] else ''))
                conn.execute(
                    'INSERT INTO edges(id,graph_id,source,target,relation,weight,descr,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)',
                    (eid, gid, body['source'], body['target'], body['relation'],
                     int(body.get('weight') or 2), body.get('desc', ''), ts, ts))
                return {'id': eid}
            return self.mutate(fn)

        if p == '/api/versions':
            body = self._body()
            def fn(conn):
                gid = require_graph(conn, body.get('graphId'))
                maybe_backup(conn, gid)
                if not body.get('name'):
                    raise ApiError('版本名必填')
                validate_window(body.get('effectiveFrom'), body.get('effectiveTo'))
                ts = now_iso()
                vid = body.get('id') or gen_id('v')
                nj, ej = snapshot_json(conn, gid)
                conn.execute(
                    'INSERT INTO versions(id,graph_id,name,descr,effective_from,effective_to,ts,nodes_json,edges_json) VALUES(?,?,?,?,?,?,?,?,?)',
                    (vid, gid, body['name'], body.get('desc', ''),
                     body.get('effectiveFrom'), body.get('effectiveTo'), ts, nj, ej))
                conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('current_version',?)",
                             (body['name'],))
                return {'id': vid}
            return self.mutate(fn)

        m = __import__('re').fullmatch(r'/api/versions/([^/]+)/restore', p)
        if m:
            vid = m.group(1)
            def fn(conn):
                r = conn.execute('SELECT * FROM versions WHERE id=? AND deleted_at IS NULL', (vid,)).fetchone()
                if not r:
                    raise ApiError('版本不存在或已在回收站中')
                gid = r['graph_id'] or require_graph(conn, self._query_graph())
                maybe_backup(conn, gid)
                replace_graph(conn, gid, json.loads(r['nodes_json']), json.loads(r['edges_json']))
                conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('current_version',?)", (r['name'],))
                return {'restored': r['name'], 'graphId': gid}
            return self.mutate(fn)

        m = __import__('re').fullmatch(r'/api/backups/([^/]+)/restore', p)
        if m:
            bid = m.group(1)
            def fn(conn):
                r = conn.execute('SELECT * FROM backups WHERE id=?', (bid,)).fetchone()
                if not r:
                    raise ApiError('备份不存在')
                gid = r['graph_id'] or require_graph(conn, self._query_graph())
                maybe_backup(conn, gid)
                replace_graph(conn, gid, json.loads(r['nodes_json']), json.loads(r['edges_json']))
                conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('current_version','自动备份恢复')")
                return {'restored': r['ts'], 'graphId': gid}
            return self.mutate(fn)

        if p == '/api/import':
            body = self._body()
            if not isinstance(body.get('nodes'), list) or not isinstance(body.get('edges'), list):
                return self._json({'error': '格式错误：缺少 nodes/edges 数组'}, 400)
            def fn(conn):
                # 图谱集合：导入数据带 graphs 则整体替换；否则沿用现有（无则建兜底图谱）
                if isinstance(body.get('graphs'), list) and body['graphs']:
                    conn.execute('DELETE FROM graphs')
                    for g in body['graphs']:
                        conn.execute(
                            'INSERT OR REPLACE INTO graphs(id,name,descr,created_at) VALUES(?,?,?,?)',
                            (g.get('id') or gen_id('g'), g.get('name', '未命名'),
                             g.get('descr', ''), g.get('createdAt', now_iso())))
                gid_set = {r['id'] for r in conn.execute('SELECT id FROM graphs')}
                if not gid_set:
                    gid_set = {create_graph(conn, '导入数据')}
                fallback = next(iter(sorted(gid_set)))
                for cand in (body.get('currentGraphId'), body.get('currentGraph')):
                    if cand in gid_set:
                        fallback = cand
                        break

                def pick_graph(item):
                    """行内 graphId/graph_id 任一合法则用之，否则落到 fallback"""
                    for key in ('graphId', 'graph_id'):
                        v = item.get(key)
                        if v in gid_set:
                            return v
                    return fallback

                for g0 in gid_set:
                    maybe_backup(conn, g0)
                conn.execute('DELETE FROM edges')
                conn.execute('DELETE FROM nodes')
                conn.execute('DELETE FROM versions')
                ts = now_iso()
                for n in body['nodes']:
                    g = pick_graph(n)
                    conn.execute(
                        'INSERT OR REPLACE INTO nodes(id,graph_id,label,category,descr,tags,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)',
                        (n.get('id') or gen_id('n'), g, n.get('label', '未命名'),
                         n.get('category', '概念'), n.get('desc', ''),
                         json.dumps(n.get('tags', []), ensure_ascii=False),
                         n.get('createdAt', ts), ts))
                for e in body['edges']:
                    g = pick_graph(e)
                    conn.execute(
                        'INSERT OR REPLACE INTO edges(id,graph_id,source,target,relation,weight,descr,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)',
                        (e.get('id') or gen_id('e'), g, e.get('source'), e.get('target'),
                         e.get('relation', '关联'), e.get('weight', 2), e.get('desc', ''),
                         e.get('createdAt', ts), ts))
                nver = 0
                if isinstance(body.get('versions'), list):
                    for v in body['versions']:
                        g = pick_graph(v)
                        conn.execute(
                            'INSERT OR REPLACE INTO versions(id,graph_id,name,descr,effective_from,effective_to,ts,nodes_json,edges_json,deleted_at) VALUES(?,?,?,?,?,?,?,?,?,NULL)',
                            (v.get('id') or gen_id('v'), g, v.get('name', '未命名'),
                             v.get('desc', ''), v.get('effectiveFrom'), v.get('effectiveTo'),
                             v.get('timestamp', ts),
                             json.dumps(v.get('nodes', []), ensure_ascii=False),
                             json.dumps(v.get('edges', []), ensure_ascii=False)))
                        nver += 1
                # 回收站内容随冷备还原（保持逻辑删除状态）
                ntrash = 0
                if isinstance(body.get('trash'), list):
                    gnames_alive = gid_set
                    for it in body['trash']:
                        typ, iid = it.get('type'), it.get('id')
                        g = it.get('graphId')
                        if g not in gnames_alive:
                            g = fallback
                        if typ == 'node':
                            conn.execute(
                                'INSERT OR REPLACE INTO nodes(id,graph_id,label,category,descr,tags,created_at,updated_at,deleted_at) VALUES(?,?,?,?,?,?,?,?,?)',
                                (iid or gen_id('n'), g, it.get('label', '未命名'),
                                 it.get('category', '概念'), it.get('desc', ''),
                                 json.dumps(it.get('tags', []), ensure_ascii=False),
                                 it.get('createdAt', ts), ts, it.get('deletedAt', ts)))
                        elif typ == 'edge':
                            conn.execute(
                                'INSERT OR REPLACE INTO edges(id,graph_id,source,target,relation,weight,descr,created_at,updated_at,deleted_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                                (iid or gen_id('e'), g, it.get('source'), it.get('target'),
                                 it.get('relation', '关联'), it.get('weight', 2), it.get('desc', ''),
                                 it.get('createdAt', ts), ts, it.get('deletedAt', ts)))
                        elif typ == 'version':
                            conn.execute(
                                'INSERT OR REPLACE INTO versions(id,graph_id,name,descr,effective_from,effective_to,ts,nodes_json,edges_json,deleted_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                                (iid or gen_id('v'), g, it.get('name', '未命名'),
                                 it.get('desc', ''), it.get('effectiveFrom'), it.get('effectiveTo'),
                                 it.get('timestamp', ts),
                                 json.dumps(it.get('nodes', []), ensure_ascii=False),
                                 json.dumps(it.get('edges', []), ensure_ascii=False),
                                 it.get('deletedAt', ts)))
                        elif typ == 'graph':
                            conn.execute(
                                'INSERT OR REPLACE INTO graphs(id,name,descr,created_at,deleted_at) VALUES(?,?,?,?,?)',
                                (iid or gen_id('g'), it.get('name', '未命名'),
                                 it.get('descr', ''), it.get('createdAt', ts), it.get('deletedAt', ts)))
                        else:
                            continue
                        ntrash += 1
                if body.get('currentVersion'):
                    conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('current_version',?)",
                                 (body['currentVersion'],))
                return {'nodes': len(body['nodes']), 'edges': len(body['edges']),
                        'versions': nver, 'graphs': len(gid_set), 'trash': ntrash}
            return self.mutate(fn)

        if p == '/api/trash/restore':
            body = self._body()
            def fn(conn):
                msg = trash_restore(conn, body.get('type'), body.get('id'))
                return {'restored': msg}
            return self.mutate(fn)

        if p == '/api/trash/purge':
            body = self._body()
            def fn(conn):
                trash_purge(conn, body.get('type'), body.get('id'))
                return {'purged': body.get('id')}
            return self.mutate(fn)

        if p == '/api/trash/empty':
            def fn(conn):
                items = trash_items(conn)
                for it in items:
                    trash_purge(conn, it['type'], it['id'])
                return {'purged': len(items)}
            return self.mutate(fn)

        if p == '/api/reset':
            body = self._body()
            def fn(conn):
                gid = require_graph(conn, body.get('graphId'))
                maybe_backup(conn, gid)
                ts = now_iso()
                conn.execute('UPDATE edges SET deleted_at=?, updated_at=? WHERE graph_id=? AND deleted_at IS NULL', (ts, ts, gid))
                conn.execute('UPDATE nodes SET deleted_at=?, updated_at=? WHERE graph_id=? AND deleted_at IS NULL', (ts, ts, gid))
                seed_gid = conn.execute(
                    "SELECT value FROM meta WHERE key='seed_graph_id'").fetchone()
                if seed_gid and seed_gid['value'] == gid:
                    seed_db(conn, gid)
                    return {'reset': gid, 'reseeded': True}
                return {'reset': gid, 'reseeded': False}
            return self.mutate(fn)

        return self._json({'error': 'not found'}, 404)

    # ---- PUT ----
    def do_PUT(self):
        p = urlparse(self.path).path

        m = __import__('re').fullmatch(r'/api/graphs/([^/]+)', p)
        if m:
            ggid, body = m.group(1), self._body()
            def fn(conn):
                r = conn.execute('SELECT * FROM graphs WHERE id=?', (ggid,)).fetchone()
                if not r:
                    raise ApiError('图谱不存在')
                conn.execute('UPDATE graphs SET name=?,descr=? WHERE id=?',
                             (body.get('name', r['name']).strip() or r['name'],
                              body.get('descr', r['descr']), ggid))
                return {'id': ggid}
            return self.mutate(fn)

        m = __import__('re').fullmatch(r'/api/nodes/([^/]+)', p)
        if m:
            nid, body = m.group(1), self._body()
            def fn(conn):
                gid = require_graph(conn, body.get('graphId'))
                r = conn.execute('SELECT * FROM nodes WHERE id=? AND graph_id=?', (nid, gid)).fetchone()
                if not r:
                    raise ApiError('节点不存在')
                check_optimistic(r, body, '节点', node_row(r))
                maybe_backup(conn, gid)
                conn.execute(
                    'UPDATE nodes SET label=?,category=?,descr=?,tags=?,updated_at=? WHERE id=?',
                    (body.get('label', r['label']), body.get('category', r['category']),
                     body.get('desc', r['descr']),
                     json.dumps(body.get('tags', json.loads(r['tags'] or '[]')), ensure_ascii=False),
                     now_iso(), nid))
                return {'id': nid}
            return self.mutate(fn)

        m = __import__('re').fullmatch(r'/api/edges/([^/]+)', p)
        if m:
            eid, body = m.group(1), self._body()
            def fn(conn):
                gid = require_graph(conn, body.get('graphId'))
                r = conn.execute('SELECT * FROM edges WHERE id=? AND graph_id=?', (eid, gid)).fetchone()
                if not r:
                    raise ApiError('关联不存在')
                check_optimistic(r, body, '关联', edge_row(r))
                maybe_backup(conn, gid)
                src = body.get('source', r['source'])
                tgt = body.get('target', r['target'])
                if src == tgt:
                    raise ApiError('不能关联自身')
                conn.execute(
                    'UPDATE edges SET source=?,target=?,relation=?,weight=?,descr=?,updated_at=? WHERE id=?',
                    (src, tgt, body.get('relation', r['relation']),
                     int(body.get('weight') or r['weight'] or 2),
                     body.get('desc', r['descr']), now_iso(), eid))
                return {'id': eid}
            return self.mutate(fn)

        m = __import__('re').fullmatch(r'/api/versions/([^/]+)', p)
        if m:
            vid, body = m.group(1), self._body()
            def fn(conn):
                gid = require_graph(conn, body.get('graphId'))
                r = conn.execute('SELECT * FROM versions WHERE id=? AND graph_id=?', (vid, gid)).fetchone()
                if not r:
                    raise ApiError('版本不存在')
                ef = body.get('effectiveFrom', r['effective_from'])
                et = body.get('effectiveTo', r['effective_to'])
                validate_window(ef, et)
                conn.execute(
                    'UPDATE versions SET name=?,descr=?,effective_from=?,effective_to=? WHERE id=?',
                    (body.get('name', r['name']), body.get('desc', r['descr']), ef, et, vid))
                return {'id': vid}
            return self.mutate(fn)

        return self._json({'error': 'not found'}, 404)

    # ---- DELETE（全部为逻辑删除，进回收站，可恢复） ----
    def do_DELETE(self):
        p = urlparse(self.path).path

        m = __import__('re').fullmatch(r'/api/graphs/([^/]+)', p)
        if m:
            ggid = m.group(1)
            def fn(conn):
                alive = conn.execute('SELECT COUNT(*) c FROM graphs WHERE deleted_at IS NULL').fetchone()['c']
                if alive <= 1:
                    raise ApiError('至少保留一个图谱，请先新建其他图谱再删除')
                r = conn.execute('SELECT * FROM graphs WHERE id=? AND deleted_at IS NULL', (ggid,)).fetchone()
                if not r:
                    raise ApiError('图谱不存在')
                maybe_backup(conn, ggid)
                ts = now_iso()
                soft_delete_graph(conn, ggid, ts)
                return {'deleted': r['name'], 'soft': True}
            return self.mutate(fn)

        m = __import__('re').fullmatch(r'/api/nodes/([^/]+)', p)
        if m:
            nid = m.group(1)
            q = parse_qs(urlparse(self.path).query)
            base = (q.get('base') or [None])[0]
            gid = self._query_graph()
            def fn(conn):
                gid2 = require_graph(conn, gid)
                r = conn.execute('SELECT * FROM nodes WHERE id=? AND graph_id=? AND deleted_at IS NULL', (nid, gid2)).fetchone()
                if not r:
                    raise ConflictError('该节点已被他人删除', {'id': nid, 'deleted': True})
                if base and base != r['updated_at']:
                    raise ConflictError('节点已被他人修改，删除前请先查看最新数据', node_row(r))
                maybe_backup(conn, gid2)
                ts = now_iso()
                soft_delete_node(conn, nid, gid2, ts)
                return {'deleted': nid, 'soft': True}
            return self.mutate(fn)

        m = __import__('re').fullmatch(r'/api/edges/([^/]+)', p)
        if m:
            eid = m.group(1)
            q = parse_qs(urlparse(self.path).query)
            base = (q.get('base') or [None])[0]
            gid = self._query_graph()
            def fn(conn):
                gid2 = require_graph(conn, gid)
                r = conn.execute('SELECT * FROM edges WHERE id=? AND graph_id=? AND deleted_at IS NULL', (eid, gid2)).fetchone()
                if not r:
                    raise ConflictError('该关联已被他人删除', {'id': eid, 'deleted': True})
                if base and base != r['updated_at']:
                    raise ConflictError('关联已被他人修改，删除前请先查看最新数据', edge_row(r))
                maybe_backup(conn, gid2)
                ts = now_iso()
                conn.execute('UPDATE edges SET deleted_at=?, updated_at=? WHERE id=? AND graph_id=?',
                             (ts, ts, eid, gid2))
                return {'deleted': eid, 'soft': True}
            return self.mutate(fn)

        m = __import__('re').fullmatch(r'/api/versions/([^/]+)', p)
        if m:
            vid = m.group(1)
            gid = self._query_graph()
            def fn(conn):
                gid2 = require_graph(conn, gid)
                r = conn.execute('SELECT * FROM versions WHERE id=? AND graph_id=? AND deleted_at IS NULL', (vid, gid2)).fetchone()
                if not r:
                    raise ApiError('版本不存在或已被删除')
                ts = now_iso()
                conn.execute('UPDATE versions SET deleted_at=? WHERE id=? AND graph_id=?', (ts, vid, gid2))
                return {'deleted': vid, 'soft': True}
            return self.mutate(fn)

        return self._json({'error': 'not found'}, 404)


def main():
    init_db()
    server = ThreadingHTTPServer((BIND_HOST, PORT), Handler)
    print(f'知识图谱工作台后端已启动（多图谱版）')
    print(f'  地址: http://{BIND_HOST}:{PORT}')
    print(f'  数据: {DB_PATH}')
    print(f'  按 Ctrl+C 停止')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n已停止')


if __name__ == '__main__':
    main()

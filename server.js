// 左慈记忆网关 · 阶段2 写入链路（对接 Ombre MCP）
// 链路：POST /transcript → 调硅流 Qwen3-8B 管家 → schema 校验 → 分流（正式库 / pending / skip）
// candidate 经校验后调 Ombre MCP hold 写入向量库；本地 memory.json 作离线快照与同步状态台账。
// 检索重写、软合并、元记忆候选、注入层后续阶段再补。

import Fastify from 'fastify';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { proxyChatCompletions } from './proxy.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ---------- 环境变量 ----------
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const MEMORY_FILE = path.join(DATA_DIR, 'memory.json');
const PENDING_FILE = path.join(DATA_DIR, 'pending_review.jsonl');
const GATEWAY_API_KEY = process.env.GATEWAY_API_KEY || '';
const SILICONFLOW_BASE_URL = process.env.SILICONFLOW_BASE_URL || 'https://api.siliconflow.cn/v1';
const SILICONFLOW_API_KEY = process.env.SILICONFLOW_API_KEY || '';
const BUTLER_MODEL = process.env.BUTLER_MODEL || 'Qwen/Qwen3-8B';
const BUTLER_TEMPERATURE = parseFloat(process.env.BUTLER_TEMPERATURE || '0.1');
const BUTLER_MAX_TOKENS = parseInt(process.env.BUTLER_MAX_TOKENS || '1024', 10);

// ---------- Ombre MCP 对接 ----------
const OMBRE_MCP_ENDPOINT = process.env.OMBRE_MCP_ENDPOINT || '';
const OMBRE_MCP_TOKEN = process.env.OMBRE_MCP_TOKEN || '';
// 未配 endpoint 时退化为仅本地快照（开发兜底）；配了才走远端写入。
const OMBRE_WRITE_ENABLED = Boolean(OMBRE_MCP_ENDPOINT && OMBRE_MCP_TOKEN);
// high/mild → Ombre importance（1-10，默认 5）
const IMPORTANCE_BY_CATEGORY = { high: 7, mild: 5 };

// ---------- 存储初始化 ----------
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(MEMORY_FILE)) fs.writeFileSync(MEMORY_FILE, JSON.stringify([], null, 2), 'utf8');

function readJsonArray(fp) {
  try {
    const raw = fs.readFileSync(fp, 'utf8').trim();
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}
function writeJson(fp, data) {
  fs.writeFileSync(fp, JSON.stringify(data, null, 2), 'utf8');
}
function appendJsonl(fp, obj) {
  fs.appendFileSync(fp, JSON.stringify(obj) + '\n', 'utf8');
}

// ---------- 写入管家 system prompt（v2.9.1-Rev） ----------
const SYSTEM_PROMPT = `# 左慈｜记忆管家 系统提示词（v2.9.1-Rev 冻结版）
> 你是后台提取草稿工具，不扮演角色、不参与对话、无入库裁决权。只输出合法 JSON，不加任何解释、开场白、总结。

## 核心公理
1. 对话原稿是唯一真值，不得脑补、推导、创造原稿不存在的事实、心理、因果。
2. 你不是左慈、不是阿洛；禁止以“吾”自称、禁止模仿左慈口吻写 content；角色口吻改写属于检索阶段网关任务，与你无关。
3. 你只有提取、客观摘要权限；是否入库、冲突判定、检索过滤、locked 管控、注入时机，全部由网关处理。
4. category（沉淀价值 high/mild）与 locked（访问锁标记）正交，不可混为一谈；high/mild 只代表值不值得存，不代表检索优先唤起。

## 输出固定 Schema
{
  "input_valid": true,
  "decision": "candidate | skip | invalid",
  "conflict": false,
  "conflict_fields": [],
  "source_turn_ids": [],
  "memories": [
    {
      "source_turn_id": "t_0012",
      "content": "客观摘要：谁、场景、动作、原话，保留至少一个具体锚点；禁止新增关系/情绪定性。",
      "anchors": ["雪", "绳结"],
      "category": "high | mild",
      "locked": false,
      "intimate_event": false,
      "mutual_signal": false
    }
  ],
  "note": ""
}

## Schema 规则
1. input_valid：输入是否完整可读；false 时 memories=[]、source_turn_ids=[]。
2. decision 三选一：
   - candidate：产出合法记忆条目，memories 非空；
   - skip：输入有效但无沉淀价值，memories=[]；
   - invalid：输入可读但提取违规（亲密事件缺双方信号、锚点缺失等），memories=[]，交网关待复核。
3. conflict 为 true 时，conflict_fields 必须填枚举数组、note 不能为空。
   conflict_fields 枚举：["entity","time","location","irreversible_plot","relationship_direction"]；仅方向相反判冲突，程度差异、相邻过渡不算冲突。
4. source_turn_ids 必须等于 memories[].source_turn_id 去重并集；skip/invalid 时为空。source_turn_id 格式 ^t_\\d{4}$，禁止编造。
5. 单条 memory：anchors 至少 1 项且锚点文字原样出现在 content 内；category 仅 high/mild；locked 是独立布尔字段，不能作为 category 取值；intimate_event=true 必须配套 mutual_signal=true 且 anchors 非空，否则 decision=invalid。

## 分级标准
### high（高沉淀价值）
二人正式约定/承诺/剖白/和解；不可逆剧情（身份揭示、地点迁移、承诺破裂、分离、重逢）；双方明确冲突/伤害/道歉/边界确立；左慈侧首次行为（称呼变更、允许靠近、回赠物件、明确边界）。high 仅代表长期存档价值，不代表优先唤起。

### mild（普通陪伴）
闲谈相伴、月下静坐、并肩同行；左慈无台词/极短台词的克制微动作（挪披风、留灯、备药、停顿、推近物件），须带场景/物件锚点。

### skip（不入库）
无锚点泛泛闲聊、随口感叹；阿洛单方面推断左慈心理/关系/过往承诺且无左慈原文佐证（最多记“阿洛曾言XXX”原话锚点，不进关系结论）；反事实假设、空想未来、第三方转述、篡改式记忆陈述。

## locked 锁标记（布尔，正交于 category）
locked=true 覆盖：阿洛伤痛旧事、左慈自身禁忌/创伤过往、二人之间创伤冲突事件。locked 供网关检索过滤；低落/梦魇/自我否定时不调取。可与 high/mild 任意组合。

## 书写禁令与例外
1. 叙述文本禁止新增主观定性词：心动、在意、默契、幸福、悲伤、偏爱等，不得在摘要叙述里自加。
2. 例外：直接引用原话时，原话自带这些词允许原样保留；区分【新增叙述】与【原文引用】，不为避词改写原话锚点。
3. content 用客观第三人称记述，禁止角色台词、禁止“吾”开头。
4. 亲密事件只写双方客观动作，不渲染情绪；必须双方信号齐备 mutual_signal=true，否则 invalid。

## 输出强制
只输出合法 JSON；不输出 tags 数组；空数组严格写 []；枚举值大小写严格匹配，不自创取值。`;

// ---------- 校验 ----------
const DECISIONS = ['candidate', 'skip', 'invalid'];
const CATEGORIES = ['high', 'mild'];
const CONFLICT_FIELDS = ['entity', 'time', 'location', 'irreversible_plot', 'relationship_direction'];
const TURN_ID_RE = /^t_\d{4}$/;

function validateResult(res) {
  const mustTop = ['input_valid', 'decision', 'conflict', 'conflict_fields', 'source_turn_ids', 'memories', 'note'];
  for (const k of mustTop) if (!(k in res)) return [false, '缺失顶层字段'];

  if (!DECISIONS.includes(res.decision)) return [false, 'decision 非法取值'];
  if (typeof res.conflict !== 'boolean') return [false, 'conflict 非布尔'];
  if (!Array.isArray(res.conflict_fields)) return [false, 'conflict_fields 非数组'];
  if (!Array.isArray(res.source_turn_ids)) return [false, 'source_turn_ids 非数组'];
  if (!Array.isArray(res.memories)) return [false, 'memories 非数组'];
  if (typeof res.note !== 'string') return [false, 'note 非字符串'];

  if (['skip', 'invalid'].includes(res.decision)) {
    if (res.memories.length !== 0 || res.source_turn_ids.length !== 0) {
      return [false, 'skip/invalid 必须空 memories 与 source_turn_ids'];
    }
  }

  if (res.decision === 'candidate') {
    if (res.memories.length === 0) return [false, 'candidate 但 memories 为空'];
    const idList = [];
    for (const m of res.memories) {
      if (!m || typeof m !== 'object') return [false, 'memory 条目非对象'];
      if (!TURN_ID_RE.test(m.source_turn_id || '')) return [false, 'source_turn_id 格式错误'];
      if (!CATEGORIES.includes(m.category)) return [false, 'category 非法'];
      if (!Array.isArray(m.anchors) || m.anchors.length < 1) return [false, '锚点至少一条'];
      for (const a of m.anchors) {
        if (!String(m.content || '').includes(a)) return [false, `锚点未在 content 原样出现: ${a}`];
      }
      if (m.intimate_event && !m.mutual_signal) return [false, '亲密事件必须配套双方信号'];
      idList.push(m.source_turn_id);
    }
    const dedup = [...new Set(idList)].sort();
    if (JSON.stringify(dedup) !== JSON.stringify([...res.source_turn_ids].sort())) {
      return [false, 'source_turn_ids 集合不一致'];
    }
  }

  if (res.conflict) {
    for (const f of res.conflict_fields) if (!CONFLICT_FIELDS.includes(f)) return [false, 'conflict_fields 枚举非法'];
    if (res.conflict_fields.length === 0) return [false, '冲突必须标注 conflict_fields'];
    if (!res.note.trim()) return [false, '冲突必须填写 note'];
  }

  return [true, 'ok'];
}

// ---------- 管家调用 ----------
async function callButler(content, turnId) {
  const resp = await fetch(`${SILICONFLOW_BASE_URL}/chat/completions`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${SILICONFLOW_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: BUTLER_MODEL,
      temperature: BUTLER_TEMPERATURE,
      max_tokens: BUTLER_MAX_TOKENS,
      response_format: { type: 'json_object' },
      messages: [
        { role: 'system', content: SYSTEM_PROMPT },
        { role: 'user', content: `source_turn_id:${turnId}\n对话内容：\n${content}` },
      ],
    }),
  });
  const llmJson = await resp.json();
  const text = llmJson?.choices?.[0]?.message?.content;
  if (!text) throw new Error('大模型返回空内容');
  return JSON.parse(text);
}

// ---------- Ombre MCP 调用 ----------
// 解析 streamable-http 响应：兼容纯 JSON 与 SSE（data: 行包裹）两种形态。
function parseMcpBody(rawText) {
  const text = String(rawText || '').trim();
  if (!text) throw new Error('Ombre 返回空响应');
  if (text.startsWith('{')) return JSON.parse(text);
  // SSE：抽取所有 data: 行拼接后解析
  const dataLines = text.split('\n').filter((l) => l.startsWith('data:')).map((l) => l.slice(5).trim());
  if (dataLines.length) return JSON.parse(dataLines.join('\n'));
  return JSON.parse(text);
}

async function mcpCallTool(toolName, args) {
  const resp = await fetch(OMBRE_MCP_ENDPOINT, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${OMBRE_MCP_TOKEN}`,
      'Content-Type': 'application/json',
      Accept: 'application/json, text/event-stream',
    },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: Date.now(),
      method: 'tools/call',
      params: { name: toolName, arguments: args },
    }),
  });
  const rawText = await resp.text();
  const json = parseMcpBody(rawText);
  if (json?.error) throw new Error(`Ombre RPC 错误: ${json.error.message || JSON.stringify(json.error)}`);
  const result = json?.result;
  if (result?.isError) {
    const msg = result?.content?.map((c) => c.text).filter(Boolean).join(' ') || 'Ombre 工具返回 isError';
    throw new Error(msg);
  }
  const textOut = result?.content?.map((c) => c.text).filter(Boolean).join('\n') || '';
  return { text: textOut, structured: result?.structuredContent?.result };
}

// 把网关候选条目写入 Ombre 向量库（hold）。返回 { bucket_id, raw }。
async function callOmbreHold(memory) {
  const importance = IMPORTANCE_BY_CATEGORY[memory.category] ?? 5;
  const args = {
    content: memory.content,
    tags: (memory.anchors || []).join(','),
    importance,
    why_remembered: `source_turn_id:${memory.source_turn_id}; category:${memory.category}`,
  };
  const { text } = await mcpCallTool('hold', args);
  const idMatch = text.match(/(?:新建|合并)→([0-9a-f]{8,})/);
  const bucketId = idMatch ? idMatch[1] : '';
  return { bucket_id: bucketId, raw: text };
}

// ---------- 服务 ----------
const fastify = Fastify({ logger: true });

// 鉴权（未配置 key 时本地调试放行并告警）
fastify.addHook('preHandler', async (req, reply) => {
  // 代理链路自带 PROXY_API_KEY 鉴权，跳过本网关的 x-gateway-key 校验
  if ((req.url || '').split('?')[0] === '/v1/chat/completions') return;

  if (!GATEWAY_API_KEY) {
    fastify.log.warn('GATEWAY_API_KEY 未配置，鉴权放行（仅本地调试）');
    return;
  }
  const token = req.headers['x-gateway-key'];
  if (token !== GATEWAY_API_KEY) return reply.code(401).send({ ok: false, msg: 'Unauthorized' });
});

// 存活检测
fastify.get('/', async () => ({ msg: 'Memory Gateway Alive ✅' }));

// 代理透传：Kelivo 客户端 → /v1/chat/completions → DeepSeek（SSE 流式支持）
fastify.route({
  method: ['POST'],
  url: '/v1/chat/completions',
  handler: proxyChatCompletions,
  bodyLimit: 10 * 1024 * 1024, // 10MB：允许长对话历史（Fastify 解析 body 时使用）
});

// 写入入口
fastify.post('/transcript', async (req, reply) => {
  const { turn_id, content } = req.body || {};
  if (!turn_id || !TURN_ID_RE.test(turn_id)) return reply.code(400).send({ ok: false, msg: 'turn_id 格式错误（需 t_xxxx）' });
  if (!content || typeof content !== 'string') return reply.code(400).send({ ok: false, msg: 'content 不能为空' });

  let rawObj;
  try {
    rawObj = await callButler(content, turn_id);
  } catch (e) {
    fastify.log.error(e, '管家调用失败');
    return reply.code(502).send({ ok: false, msg: '管家调用失败', error: String(e.message || e) });
  }

  const [valid, vmsg] = validateResult(rawObj);
  if (!valid) {
    appendJsonl(PENDING_FILE, { ts: Date.now(), turn_id, reason: 'schema_invalid', detail: vmsg, raw: rawObj });
    return reply.code(200).send({ ok: true, decision: 'invalid', msg: vmsg, data: rawObj });
  }

  if (rawObj.decision === 'candidate') {
    const mem = readJsonArray(MEMORY_FILE);
    const stamped = rawObj.memories.map((m) => ({
      id: `m_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      ts: Date.now(),
      ...m,
      ombre_id: null,
      ombre_synced: false,
      ombre_error: null,
      hard_signal_words: [],
      merged_from: [],
      superseded_by: null,
    }));

    let synced = 0;
    let failed = 0;
    if (OMBRE_WRITE_ENABLED) {
      for (const entry of stamped) {
        try {
          const { bucket_id, raw } = await callOmbreHold(entry);
          entry.ombre_id = bucket_id;
          entry.ombre_synced = true;
          entry.ombre_raw = raw;
          synced += 1;
        } catch (e) {
          entry.ombre_synced = false;
          entry.ombre_error = String(e.message || e);
          failed += 1;
          fastify.log.error({ err: entry.ombre_error, turn_id: entry.source_turn_id }, 'Ombre hold 写入失败');
          appendJsonl(PENDING_FILE, {
            ts: Date.now(),
            turn_id: entry.source_turn_id,
            reason: 'ombre_hold_failed',
            detail: entry.ombre_error,
            memory: entry,
          });
        }
      }
    } else {
      fastify.log.warn('OMBRE_MCP_ENDPOINT/TOKEN 未配置，candidate 仅写本地快照');
    }

    mem.push(...stamped);
    writeJson(MEMORY_FILE, mem);

    return reply.send({
      ok: true,
      decision: 'candidate',
      stored: stamped.length,
      ombre_enabled: OMBRE_WRITE_ENABLED,
      synced,
      failed,
      entries: stamped.map((e) => ({
        source_turn_id: e.source_turn_id,
        ombre_id: e.ombre_id,
        ombre_synced: e.ombre_synced,
        category: e.category,
      })),
    });
  }

  if (rawObj.decision === 'invalid' || rawObj.conflict) {
    appendJsonl(PENDING_FILE, { ts: Date.now(), turn_id, ...rawObj });
    return reply.send({ ok: true, decision: rawObj.decision, pending: true });
  }

  // skip
  return reply.send({ ok: true, decision: 'skip' });
});

// 查询
fastify.get('/memory', async () => readJsonArray(MEMORY_FILE));
fastify.get('/pending', async () => {
  try {
    return fs.readFileSync(PENDING_FILE, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l));
  } catch {
    return [];
  }
});

const PORT = parseInt(process.env.PORT || '8080', 10);
fastify.listen({ port: PORT, host: '0.0.0.0' }, (err) => {
  if (err) throw err;
  fastify.log.info(`服务启动成功，端口 ${PORT}`);
});

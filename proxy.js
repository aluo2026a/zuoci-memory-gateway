/**
 * proxy.js — 透传代理（Fastify 版，demo / ombre 双模式记忆注入）
 *
 * 职责：
 *  1. 接收 Kelivo 客户端发来的 POST /v1/chat/completions 请求
 *  2. 使用 PROXY_API_KEY 做 Bearer Token 鉴权（fail-closed）
 *  3. 将请求体与其它头部原样转发到上游 DeepSeek API，
 *     并把 Authorization 替换为 DEEPSEEK_API_KEY
 *  4. 支持 SSE 流式透传（stream: true）
 *  5. INJECT_MODE=demo ：把测试记忆拼到最后一个 user 消息前，响应头 X-Proxy-Memory-Injected: true
 *  6. INJECT_MODE=ombre：调用 Ombre MCP breath_search 检索真实记忆并注入；
 *     检索失败/无命中/超时(3s) 静默跳过，原样透传，保证不卡主对话。
 *
 * 环境变量：
 *  - PROXY_API_KEY      必填：客户端访问本网关用的 Key
 *  - DEEPSEEK_API_KEY   必填：转发给上游 DeepSeek 的真实 Key（主对话模型）
 *  - UPSTREAM_API_KEY   可选：覆盖上游 Key（默认取 DEEPSEEK_API_KEY）
 *  - UPSTREAM_HOST      可选：默认 api.deepseek.com
 *  - UPSTREAM_BASE_PATH 可选：默认 /v1
 *  - INJECT_MODE        可选：passthrough（默认）| demo | ombre
 *  - OMBRE_MCP_ENDPOINT ombre 模式必填：Ombre MCP 端点（streamable-http）
 *  - OMBRE_MCP_TOKEN    ombre 模式必填：Ombre MCP Bearer Token
 *
 * 关键说明：
 *  - Fastify 会把 application/json 的 body 解析到 request.body，直接用它。
 *  - 响应用 Fastify 原生 reply.send(stream) 透传 SSE，不用 reply.hijack，
 *    避免 FST_ERR_REP_ALREADY_SENT。
 *  - 所有进 Header 的敏感值都经 sanitizeHeaderValue 清洗，避免非法字符。
 *  - 依赖：Node 18+ 的全局 fetch 与 AbortController，无需额外 npm 包。
 */

import https from 'node:https';

/**
 * 严格清洗 Header 值：去换行/制表符 → trim → 只保留可打印 ASCII。
 */
function sanitizeHeaderValue(value) {
  return String(value ?? '')
    .replace(/[\r\n\t]/g, '')
    .trim()
    .replace(/[^\x20-\x7E]/g, '');
}

const PROXY_API_KEY = sanitizeHeaderValue(process.env.PROXY_API_KEY || '');
const INJECT_MODE = (process.env.INJECT_MODE || 'passthrough').trim();
const UPSTREAM_HOST = (process.env.UPSTREAM_HOST || 'api.deepseek.com').trim();
const UPSTREAM_BASE_PATH = (process.env.UPSTREAM_BASE_PATH || '/v1').replace(/\/+$/, '');

const OMBRE_MCP_ENDPOINT = (process.env.OMBRE_MCP_ENDPOINT || '').trim();
const OMBRE_MCP_TOKEN = sanitizeHeaderValue(process.env.OMBRE_MCP_TOKEN || '');
const OMBRE_SEARCH_TIMEOUT_MS = 3000; // Ombre 检索最长 3 秒，超时静默跳过

const UPSTREAM_TIMEOUT_MS = 300000; // 上游 5 分钟无数据才超时（推理模型首 token 可能较慢）

// demo 模式要注入的测试记忆
const DEMO_MEMORY = '【过往史实，非当前对话】雪落之日，他曾为阿洛系稳绳结，话很少。';
const MEMORY_PREFIX = '【过往史实，非当前对话】';

if (INJECT_MODE === 'demo') {
  console.log('[proxy] demo 模式已启用：注入测试记忆，响应头返回 X-Proxy-Memory-Injected: true');
} else if (INJECT_MODE === 'ombre') {
  console.log('[proxy] ombre 模式已启用：' +
    (OMBRE_MCP_ENDPOINT && OMBRE_MCP_TOKEN ? 'Ombre 检索已就绪' : '缺少 OMBRE_MCP_ENDPOINT/TOKEN，将退化为透传'));
} else if (INJECT_MODE !== 'passthrough') {
  console.warn('[proxy] 未知 INJECT_MODE=' + INJECT_MODE + '，按 passthrough 处理');
}

function extractBearerToken(headers) {
  const auth = headers['authorization'] || '';
  const match = /^Bearer\s+(.+)$/i.exec(auth);
  const raw = match ? match[1] : auth;
  return sanitizeHeaderValue(raw);
}

function sendError(reply, statusCode, message) {
  reply.header('X-Proxy-Memory-Injected', 'false');
  reply.code(statusCode).send({ error: { message } });
}

/** 取最后一条 user 消息的 content（用于检索 query）。 */
function getLastUserContent(bodyObj) {
  if (!bodyObj || !Array.isArray(bodyObj.messages)) return '';
  for (let i = bodyObj.messages.length - 1; i >= 0; i--) {
    const m = bodyObj.messages[i];
    if (m && m.role === 'user' && typeof m.content === 'string') {
      return m.content;
    }
  }
  return '';
}

/** 把 text 拼到最后一个 user 消息 content 的最前面。 */
function prependToLastUser(bodyObj, text) {
  if (!bodyObj || !Array.isArray(bodyObj.messages) || !text) return bodyObj;
  for (let i = bodyObj.messages.length - 1; i >= 0; i--) {
    const m = bodyObj.messages[i];
    if (m && m.role === 'user' && typeof m.content === 'string') {
      m.content = text + '\n\n' + m.content;
      console.log('[proxy] 记忆已注入最后一条 user 消息');
      break;
    }
  }
  return bodyObj;
}

/** 解析 streamable-http 响应：兼容纯 JSON 与 SSE（data: 行包裹）。 */
function parseMcpBody(rawText) {
  const text = String(rawText || '').trim();
  if (!text) throw new Error('Ombre 返回空响应');
  if (text.startsWith('{')) return JSON.parse(text);
  const dataLines = text.split('\n').filter((l) => l.startsWith('data:')).map((l) => l.slice(5).trim());
  if (dataLines.length) return JSON.parse(dataLines.join('\n'));
  return JSON.parse(text);
}

/** 从 MCP 结果里尽量提取记忆文本（兼容 structuredContent 与 content 文本块）。 */
function extractSearchText(result) {
  const structured = result?.structuredContent?.result;
  if (structured != null) {
    if (typeof structured === 'string') return structured.trim();
    if (Array.isArray(structured)) {
      return structured
        .map((item) => (typeof item === 'string' ? item : (item?.content || item?.text || item?.memory || '')))
        .filter(Boolean)
        .join('\n')
        .trim();
    }
    if (typeof structured === 'object') {
      const v = structured.content || structured.text || structured.memories || structured.results;
      if (typeof v === 'string') return v.trim();
      if (Array.isArray(v)) return v.map(String).join('\n').trim();
      if (v != null) return JSON.stringify(v);
    }
  }
  const text = result?.content?.map((c) => c.text).filter(Boolean).join('\n').trim() || '';
  return text;
}

/**
 * 调用 Ombre MCP 的 breath_search 工具，检索 query。
 * 任何失败/无命中/超时都返回空字符串，绝不抛错（静默跳过）。
 */
async function searchOmbre(query) {
  if (!OMBRE_MCP_ENDPOINT || !OMBRE_MCP_TOKEN || !query) return '';

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), OMBRE_SEARCH_TIMEOUT_MS);

  try {
    const resp = await fetch(OMBRE_MCP_ENDPOINT, {
      method: 'POST',
      headers: {
        Authorization: 'Bearer ' + OMBRE_MCP_TOKEN,
        'Content-Type': 'application/json',
        Accept: 'application/json, text/event-stream',
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: Date.now(),
        method: 'tools/call',
        // 若 Ombre breath_search 的参数名不是 query，改这里
        params: { name: 'breath_search', arguments: { query } },
      }),
      signal: controller.signal,
    });

    const rawText = await resp.text();
    const json = parseMcpBody(rawText);
    if (json?.error) throw new Error(json.error.message || JSON.stringify(json.error));

    const result = json?.result;
    if (result?.isError) {
      const msg = result?.content?.map((c) => c.text).filter(Boolean).join(' ') || 'Ombre 工具返回 isError';
      throw new Error(msg);
    }

    return extractSearchText(result);
  } catch (err) {
    console.warn('[proxy] Ombre breath_search 失败/超时，静默跳过：', err.message || String(err));
    return '';
  } finally {
    clearTimeout(timer);
  }
}

/** 发起到上游 DeepSeek 的请求，resolve 上游响应流（IncomingMessage）。 */
function requestUpstream(body, upstreamHeaders) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        host: UPSTREAM_HOST,
        path: UPSTREAM_BASE_PATH + '/chat/completions',
        method: 'POST',
        headers: upstreamHeaders,
      },
      (res) => resolve(res)
    );
    req.on('error', reject);
    req.setTimeout(UPSTREAM_TIMEOUT_MS, () => req.destroy(new Error('upstream idle timeout')));
    req.write(body);
    req.end();
  });
}

/**
 * Fastify 路由处理器（async，不使用 reply.hijack）。
 * 通过 return 上游流（reply.send(stream)）透传 SSE。
 */
export async function proxyChatCompletions(request, reply) {
  const headers = request.headers;

  // 1) PROXY_API_KEY 鉴权
  if (!PROXY_API_KEY) {
    console.error('[proxy] PROXY_API_KEY 未配置，拒绝请求（fail-closed）');
    sendError(reply, 503, 'PROXY_API_KEY is not configured on the gateway');
    return;
  }
  const token = extractBearerToken(headers);
  if (!token || token !== PROXY_API_KEY) {
    console.warn('[proxy] 鉴权失败');
    sendError(reply, 401, 'Unauthorized: invalid or missing proxy API key');
    return;
  }

  const upstreamApiKey = sanitizeHeaderValue(process.env.UPSTREAM_API_KEY || process.env.DEEPSEEK_API_KEY || '');
  if (!upstreamApiKey) {
    console.error('[proxy] DEEPSEEK_API_KEY 未配置，无法转发到上游');
    sendError(reply, 503, 'DEEPSEEK_API_KEY is not configured on the gateway');
    return;
  }

  // 2) 组装请求体
  let bodyObj = request.body ?? {};
  let memoryInjected = false;

  if (INJECT_MODE === 'demo') {
    bodyObj = prependToLastUser(bodyObj, DEMO_MEMORY);
    memoryInjected = true;
  } else if (INJECT_MODE === 'ombre') {
    const query = getLastUserContent(bodyObj);
    if (query) {
      const fragments = await searchOmbre(query);
      if (fragments) {
        bodyObj = prependToLastUser(bodyObj, MEMORY_PREFIX + fragments);
        memoryInjected = true;
      }
    }
  }

  const body = Buffer.from(JSON.stringify(bodyObj), 'utf8');

  // 3) 原样透传请求头，剔除需要重写的头
  const upstreamHeaders = {};
  for (const key of Object.keys(headers)) {
    const value = headers[key];
    if (value === undefined) continue;
    const lower = key.toLowerCase();
    if ([
      'host', 'connection', 'content-length', 'transfer-encoding',
      'authorization', 'expect', 'keep-alive', 'proxy-connection',
    ].includes(lower)) continue;
    upstreamHeaders[key] = value;
  }
  upstreamHeaders['Host'] = UPSTREAM_HOST;
  upstreamHeaders['Authorization'] = sanitizeHeaderValue('Bearer ' + upstreamApiKey);
  upstreamHeaders['Content-Length'] = body.length;

  // 4) 转发到上游 DeepSeek
  let upstreamRes;
  try {
    upstreamRes = await requestUpstream(body, upstreamHeaders);
  } catch (err) {
    console.error('[proxy] 转发上游失败：', err.message);
    sendError(reply, 502, 'Upstream request failed: ' + err.message);
    return;
  }

  // 5) 转发上游状态码与响应头
  reply.code(upstreamRes.statusCode || 502);
  for (const key of Object.keys(upstreamRes.headers)) {
    const value = upstreamRes.headers[key];
    if (value === undefined) continue;
    const lower = key.toLowerCase();
    if (['content-length', 'transfer-encoding', 'connection', 'keep-alive'].includes(lower)) continue;
    reply.header(key, value);
  }
  reply.header('X-Proxy-Memory-Injected', memoryInjected ? 'true' : 'false');

  // 6) 返回上游流，Fastify 负责发送（SSE 透传）
  return upstreamRes;
}

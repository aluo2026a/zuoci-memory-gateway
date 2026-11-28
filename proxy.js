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
 *  6. INJECT_MODE=ombre：调用 queryOmbreMemory 检索真实记忆并注入；
 *     检索失败/无命中/超时 静默跳过，原样透传。
 *
 * 环境变量：
 *  - PROXY_API_KEY      必填：客户端访问本网关用的 Key
 *  - DEEPSEEK_API_KEY   必填：转发给上游 DeepSeek 的真实 Key（主对话模型）
 *  - UPSTREAM_API_KEY   可选：覆盖上游 Key（默认取 DEEPSEEK_API_KEY）
 *  - UPSTREAM_HOST      可选：默认 api.deepseek.com
 *  - UPSTREAM_BASE_PATH 可选：默认 /v1
 *  - INJECT_MODE        可选：passthrough（默认）| demo | ombre
 *  - OMBRE_MCP_ENDPOINT ombre 模式必填（在 ombre.js 里读取）
 *  - OMBRE_MCP_TOKEN    ombre 模式必填（在 ombre.js 里读取）
 *
 * 关键说明：
 *  - Fastify 会把 application/json 的 body 解析到 request.body，直接用它，
 *    不要读 request.raw 流（req.on('end') 不会触发，会导致挂起）。
 *  - 响应用 Fastify 原生 reply.send(stream) 透传 SSE，不用 reply.hijack，
 *    避免 FST_ERR_REP_ALREADY_SENT。
 */

import https from 'node:https';
import { queryOmbreMemory } from './ombre.js';

/** 严格清洗 Header 值：去换行/制表符 → trim → 只保留可打印 ASCII。 */
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

const UPSTREAM_TIMEOUT_MS = 300000; // 上游 5 分钟无数据才超时

// 把记忆里的第三人称转成第二人称（你=阿洛）和第一人称（吾=左慈）
function toSecondPerson(text) {
  return String(text || '')
    .replace(/阿洛/g, '你')
    .replace(/左慈/g, '吾')
    .replace(/她/g, '你')
    .replace(/他/g, '吾');
}

// 把记忆做成极简第一人称状态残片：（记忆内容）
// 超过 60 字时，尽量在前 60 字内找最后一个句号/逗号截断，避免切成半个句子
function buildFragment(memoryText) {
  const text = String(memoryText || '');
  if (text.length <= 60) {
    return '（' + text + '）';
  }
  const slice = text.slice(0, 60);
  let cut = -1;
  for (const p of ['。', '！', '？', '；', '，', '、']) {
    const idx = slice.lastIndexOf(p);
    if (idx > cut) cut = idx;
  }
  const fragment = cut >= 0 ? slice.slice(0, cut + 1) : slice;
  return '（' + fragment + '）';
}

const DEMO_MEMORY_TEXT = '雪落之日，吾曾为你系稳绳结，话很少。';

if (INJECT_MODE === 'demo') {
  console.log('[proxy] demo 模式已启用：注入测试记忆');
} else if (INJECT_MODE === 'ombre') {
  console.log('[proxy] ombre 模式已启用：接入 Ombre breath_search 真实记忆检索');
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

/** 在最后一条 user 消息之前，插入一条 assistant 潜意识残片；user 消息保持原样。 */
function insertAssistantMemory(bodyObj, fragment) {
  if (!bodyObj || !Array.isArray(bodyObj.messages) || !fragment) return bodyObj;
  for (let i = bodyObj.messages.length - 1; i >= 0; i--) {
    const m = bodyObj.messages[i];
    if (m && m.role === 'user') {
      bodyObj.messages.splice(i, 0, { role: 'assistant', content: fragment });
      console.log('[proxy] 已插入 assistant 潜意识残片');
      break;
    }
  }
  return bodyObj;
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

  // 2) 组装请求体（直接用 Fastify 已解析的 request.body）
  let bodyObj = request.body ?? {};
  let memoryInjected = false;

  if (INJECT_MODE === 'demo') {
    bodyObj = insertAssistantMemory(bodyObj, buildFragment(DEMO_MEMORY_TEXT));
    memoryInjected = true;
  } else if (INJECT_MODE === 'ombre') {
    const query = getLastUserContent(bodyObj);
    if (query) {
      const { success, payload } = await queryOmbreMemory(query);
      if (success && payload) {
        bodyObj = insertAssistantMemory(bodyObj, buildFragment(toSecondPerson(payload)));
        memoryInjected = true;
      }
    }
  }

  // 重序列化后 Content-Length 自动按新 body 计算
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

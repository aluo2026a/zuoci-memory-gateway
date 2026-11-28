/**
 * proxy.js — 透传代理（Fastify 版，使用 request.body + reply.send(stream)）
 *
 * 职责：
 *  1. 接收 Kelivo 客户端发来的 POST /v1/chat/completions 请求
 *  2. 使用 PROXY_API_KEY 做 Bearer Token 鉴权（fail-closed）
 *  3. 将请求体与其它头部原样转发到上游 DeepSeek API，
 *     并把 Authorization 替换为 DEEPSEEK_API_KEY
 *  4. 支持 SSE 流式透传（stream: true）
 *  5. INJECT_MODE=demo 时：把测试记忆拼接到最后一条 user 消息前，
 *     并返回 X-Proxy-Memory-Injected: true；否则返回 false
 *
 * 环境变量：
 *  - PROXY_API_KEY      必填：客户端访问本网关用的 Key
 *  - DEEPSEEK_API_KEY   必填：转发给上游 DeepSeek 的真实 Key（主对话模型）
 *  - UPSTREAM_API_KEY   可选：覆盖上游 Key（默认取 DEEPSEEK_API_KEY）
 *  - UPSTREAM_HOST      可选：默认 api.deepseek.com
 *  - UPSTREAM_BASE_PATH 可选：默认 /v1
 *  - INJECT_MODE        可选：passthrough（默认，不注入）| demo（注入测试记忆）
 *
 * 关键说明（不再使用 reply.hijack）：
 *  - Fastify 在解析阶段会把 application/json 的 body 解析到 request.body，直接用它。
 *  - 响应用 Fastify 原生的 reply.send(stream) 透传 SSE，避免 hijack 与框架抢响应
 *    导致的 FST_ERR_REP_ALREADY_SENT。
 */

import https from 'node:https';

const PROXY_API_KEY = (process.env.PROXY_API_KEY || '').trim();
const INJECT_MODE = (process.env.INJECT_MODE || 'passthrough').trim();
const UPSTREAM_HOST = (process.env.UPSTREAM_HOST || 'api.deepseek.com').trim();
const UPSTREAM_BASE_PATH = (process.env.UPSTREAM_BASE_PATH || '/v1').replace(/\/+$/, '');

const UPSTREAM_TIMEOUT_MS = 300000; // 5 分钟无数据才超时（推理模型首 token 可能较慢）

// demo 模式要注入的测试记忆（拼接在最后一条 user 消息内容最前面）
const DEMO_MEMORY = '【过往史实，非当前对话】雪落之日，他曾为阿洛系稳绳结，话很少。';
const MEMORY_INJECTED = INJECT_MODE === 'demo';

if (INJECT_MODE === 'demo') {
  console.log('[proxy] demo 模式已启用：注入测试记忆，响应头返回 X-Proxy-Memory-Injected: true');
} else if (INJECT_MODE !== 'passthrough') {
  console.warn('[proxy] 未知 INJECT_MODE=' + INJECT_MODE + '，按 passthrough 处理');
}

function extractBearerToken(headers) {
  const auth = headers['authorization'] || '';
  const match = /^Bearer\s+(.+)$/i.exec(auth);
  return match ? match[1].trim() : auth.trim();
}

function sendError(reply, statusCode, message) {
  reply.header('X-Proxy-Memory-Injected', 'false');
  reply.code(statusCode).send({ error: { message } });
}

/**
 * demo 模式：把测试记忆拼接到最后一条 user 消息的内容最前面。
 */
function injectDemoMemory(bodyObj) {
  try {
    if (bodyObj && Array.isArray(bodyObj.messages)) {
      for (let i = bodyObj.messages.length - 1; i >= 0; i--) {
        const msg = bodyObj.messages[i];
        if (msg && msg.role === 'user') {
          if (typeof msg.content === 'string') {
            msg.content = DEMO_MEMORY + '\n\n' + msg.content;
            console.log('[proxy] demo 记忆已注入最后一条 user 消息');
          }
          break;
        }
      }
    }
  } catch (err) {
    console.error('[proxy] demo 注入失败，按原样转发：', err.message);
  }
  return bodyObj;
}

/**
 * Fastify 路由处理器（非 async，不使用 reply.hijack）。
 * 通过 reply.send(stream) 透传上游响应，支持 SSE 流式返回。
 */
export function proxyChatCompletions(request, reply) {
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

  const upstreamApiKey = (process.env.UPSTREAM_API_KEY || process.env.DEEPSEEK_API_KEY || '').trim();
  if (!upstreamApiKey) {
    console.error('[proxy] DEEPSEEK_API_KEY 未配置，无法转发到上游');
    sendError(reply, 503, 'DEEPSEEK_API_KEY is not configured on the gateway');
    return;
  }

  // 2) 组装请求体：Fastify 已把 application/json 解析到 request.body
  let bodyObj = request.body ?? {};
  if (MEMORY_INJECTED) {
    bodyObj = injectDemoMemory(bodyObj);
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
  upstreamHeaders['Authorization'] = 'Bearer ' + upstreamApiKey;
  upstreamHeaders['Content-Length'] = body.length;

  // 4) 转发到上游 DeepSeek，用 reply.send(stream) 透传响应
  const upstreamReq = https.request(
    {
      host: UPSTREAM_HOST,
      path: UPSTREAM_BASE_PATH + '/chat/completions',
      method: 'POST',
      headers: upstreamHeaders,
    },
    (upstreamRes) => {
      reply.code(upstreamRes.statusCode || 502);

      for (const key of Object.keys(upstreamRes.headers)) {
        const value = upstreamRes.headers[key];
        if (value === undefined) continue;
        const lower = key.toLowerCase();
        if (['content-length', 'transfer-encoding', 'connection', 'keep-alive'].includes(lower)) continue;
        reply.header(key, value);
      }
      reply.header('X-Proxy-Memory-Injected', MEMORY_INJECTED ? 'true' : 'false');

      reply.send(upstreamRes);
    }
  );

  upstreamReq.on('error', (err) => {
    console.error('[proxy] 转发上游失败：', err.message);
    if (!reply.sent) {
      sendError(reply, 502, 'Upstream request failed: ' + err.message);
    }
  });

  // 连接后的“无数据”超时（流式期间每个 chunk 都会重置计时）
  upstreamReq.setTimeout(UPSTREAM_TIMEOUT_MS, () => {
    upstreamReq.destroy(new Error('upstream idle timeout'));
  });

  upstreamReq.write(body);
  upstreamReq.end();
}

/**
 * proxy.js — 透传代理（Fastify 版，使用 request.body）
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
 * 关键说明（重要，避免 FST_ERR_REP_ALREADY_SENT）：
 *  - Fastify 会在解析阶段消费 application/json 的 body 并放入 request.body，
 *    因此这里直接用 request.body（解析好的对象），不去读 request.raw 流。
 *  - 响应侧用 reply.hijack() 接管做 SSE 透传。hijack 之后 reply.sent 恒为 true，
 *    所以 handler 必须返回 undefined（什么都不返回），让 Fastify 跳过
 *    preHandlerCallbackInner 里的 reply.send(result)，否则会抛 FST_ERR_REP_ALREADY_SENT。
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

function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(body),
    'X-Proxy-Memory-Injected': 'false',
  });
  res.end(body);
}

/**
 * demo 模式：把测试记忆拼接到最后一条 user 消息的内容最前面。
 * 直接原地修改解析后的对象并返回。
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
 * 实际转发逻辑（非 async）。
 * request：Fastify 的 request（用 request.body / request.headers）。
 * res：reply.raw（Node 原生 ServerResponse）。
 */
function handleChatCompletions(request, res) {
  const headers = request.headers;

  // 1) PROXY_API_KEY 鉴权
  if (!PROXY_API_KEY) {
    console.error('[proxy] PROXY_API_KEY 未配置，拒绝请求（fail-closed）');
    sendJson(res, 503, { error: { message: 'PROXY_API_KEY is not configured on the gateway' } });
    return;
  }
  const token = extractBearerToken(headers);
  if (!token || token !== PROXY_API_KEY) {
    console.warn('[proxy] 鉴权失败');
    sendJson(res, 401, { error: { message: 'Unauthorized: invalid or missing proxy API key' } });
    return;
  }

  const upstreamApiKey = (process.env.UPSTREAM_API_KEY || process.env.DEEPSEEK_API_KEY || '').trim();
  if (!upstreamApiKey) {
    console.error('[proxy] DEEPSEEK_API_KEY 未配置，无法转发到上游');
    sendJson(res, 503, { error: { message: 'DEEPSEEK_API_KEY is not configured on the gateway' } });
    return;
  }

  // 2) 组装请求体：Fastify 已把 application/json 解析到 request.body，直接用
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

  // 4) 转发到上游 DeepSeek
  const upstreamReq = https.request(
    {
      host: UPSTREAM_HOST,
      path: UPSTREAM_BASE_PATH + '/chat/completions',
      method: 'POST',
      headers: upstreamHeaders,
    },
    (upstreamRes) => {
      // 转发上游状态码与响应头
      const responseHeaders = {};
      for (const key of Object.keys(upstreamRes.headers)) {
        const value = upstreamRes.headers[key];
        if (value === undefined) continue;
        const lower = key.toLowerCase();
        if (['content-length', 'transfer-encoding', 'connection', 'keep-alive'].includes(lower)) continue;
        responseHeaders[key] = value;
      }
      responseHeaders['X-Proxy-Memory-Injected'] = MEMORY_INJECTED ? 'true' : 'false';

      res.writeHead(upstreamRes.statusCode || 502, responseHeaders);
      upstreamRes.pipe(res);
    }
  );

  upstreamReq.on('error', (err) => {
    console.error('[proxy] 转发上游失败：', err.message);
    if (!res.headersSent) {
      sendJson(res, 502, { error: { message: 'Upstream request failed: ' + err.message } });
    } else {
      res.end();
    }
  });

  // 连接后的“无数据”超时（流式期间每个 chunk 都会重置计时）
  upstreamReq.setTimeout(UPSTREAM_TIMEOUT_MS, () => {
    upstreamReq.destroy(new Error('upstream idle timeout'));
  });

  upstreamReq.write(body);
  upstreamReq.end();
}

/**
 * Fastify 路由处理器（必须非 async）。
 * reply.hijack() 接管响应后，必须返回 undefined（什么都不返回），
 * 让 Fastify 跳过 reply.send()，否则会抛 FST_ERR_REP_ALREADY_SENT。
 */
export function proxyChatCompletions(request, reply) {
  reply.hijack();
  handleChatCompletions(request, reply.raw);
  return undefined;
}

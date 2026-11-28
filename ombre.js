/**
 * ombre.js — Ombre MCP 检索封装（纯 ESM）
 *
 * 导出 queryOmbreMemory(userQuery)：调用 Ombre 的 breath_search 工具做记忆检索。
 *  - 3 秒超时（AbortController）
 *  - 任何报错 / 超时 / 无结果，一律返回 { success: false, payload: null }，绝不抛错
 *  - 命中时过滤掉 metadata.inferred === true 的条目，拼接成纯文本返回
 *      { success: true, payload: 文本 }
 *
 * 依赖：Node 18+ 的全局 fetch 与 AbortController，无需额外 npm 包。
 */

const OMBRE_MCP_ENDPOINT = (process.env.OMBRE_MCP_ENDPOINT || '').trim();
const OMBRE_MCP_TOKEN = (process.env.OMBRE_MCP_TOKEN || '').trim();
const SEARCH_TIMEOUT_MS = 3000;

/** 解析 streamable-http 响应：兼容纯 JSON 与 SSE（data: 行包裹）。 */
function parseMcpBody(rawText) {
  const text = String(rawText || '').trim();
  if (!text) throw new Error('Ombre 返回空响应');
  if (text.startsWith('{')) return JSON.parse(text);
  const dataLines = text.split('\n').filter((l) => l.startsWith('data:')).map((l) => l.slice(5).trim());
  if (dataLines.length) return JSON.parse(dataLines.join('\n'));
  return JSON.parse(text);
}

/** 从 MCP 结果里提取「非 inferred」条目的纯文本。 */
function buildPayload(result) {
  const structured = result?.structuredContent?.result ?? result?.structuredContent?.results;

  if (Array.isArray(structured)) {
    const kept = structured.filter((item) => !(item?.metadata?.inferred === true));
    const text = kept
      .map((item) => {
        if (typeof item === 'string') return item;
        return item?.content || item?.text || item?.memory || '';
      })
      .filter(Boolean)
      .join('\n')
      .trim();
    if (text) return text;
  }

  // 回退：content 文本块
  const text = result?.content?.map((c) => c.text).filter(Boolean).join('\n').trim() || '';
  return text;
}

/**
 * 检索记忆。
 * 返回 { success, payload }：
 *  - success=true 时 payload 为过滤后的纯文本；
 *  - 任何失败/超时/空结果 → { success: false, payload: null }。
 */
export async function queryOmbreMemory(userQuery) {
  if (!OMBRE_MCP_ENDPOINT || !OMBRE_MCP_TOKEN || !userQuery) {
    return { success: false, payload: null };
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), SEARCH_TIMEOUT_MS);

  try {
    const resp = await fetch(OMBRE_MCP_ENDPOINT, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${OMBRE_MCP_TOKEN}`,
        'Content-Type': 'application/json',
        Accept: 'application/json, text/event-stream',
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        method: 'tools/call',
        params: { name: 'breath_search', arguments: { query: userQuery } },
        id: Date.now(),
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

    const payload = buildPayload(result);
    if (!payload) return { success: false, payload: null };
    return { success: true, payload };
  } catch (err) {
    console.warn('[ombre] breath_search 失败/超时：', err.message || String(err));
    return { success: false, payload: null };
  } finally {
    clearTimeout(timer);
  }
}

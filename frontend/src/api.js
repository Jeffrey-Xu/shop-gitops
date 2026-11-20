// 后端接口的唯一入口。
//
// 所有请求都从这里走，目的是把三件容易散落的事集中管起来：
//   1. token 怎么带（每个调用点各写一遍 Headers 迟早会漏）
//   2. 401 怎么处理（token 过期要在全局清掉，不能每个页面各判一次）
//   3. 错误消息怎么取（后端统一返回 {error}，取不到再退回 HTTP 状态码）

const API = '/api';

const TOKEN_KEY = 'shop.token';

export function getToken() {
  return localStorage.getItem(TOKEN_KEY) || '';
}

export function setToken(next) {
  if (next) localStorage.setItem(TOKEN_KEY, next);
  else localStorage.removeItem(TOKEN_KEY);
}

// token 失效时通知外层（用于刷新登录态 UI）。
// 用回调而不是在这里 import 渲染函数，避免 api 和视图互相依赖。
let onUnauthorized = () => {};
export function setUnauthorizedHandler(fn) {
  onUnauthorized = fn;
}

export class ApiError extends Error {
  constructor(message, status) {
    super(message);
    this.status = status;
  }
}

async function request(path, opts = {}) {
  const headers = new Headers(opts.headers || {});
  const token = getToken();
  if (token) headers.set('Authorization', `Bearer ${token}`);
  if (opts.body && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }

  const res = await fetch(path, { ...opts, headers, cache: 'no-store' });

  // 401 只在「本来带着 token」时才算登录失效。
  // 匿名用户访问需要登录的接口也会拿到 401，那不是过期，不该弹提示。
  if (res.status === 401 && token) {
    setToken('');
    onUnauthorized();
  }

  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    try {
      const data = await res.json();
      if (data?.error) msg = data.error;
    } catch { /* 响应不是 JSON，用状态码 */ }
    throw new ApiError(msg, res.status);
  }
  return res;
}

async function requestJSON(path, opts) {
  const res = await request(path, opts);
  return res.json();
}

export const api = {
  async products({ category = '', q = '', sort = '' } = {}) {
    const params = new URLSearchParams();
    if (category) params.set('category', category);
    if (q) params.set('q', q);
    if (sort) params.set('sort', sort);
    const qs = params.toString();
    const res = await request(`${API}/products${qs ? '?' + qs : ''}`);
    // X-Cache 是演示缓存是否命中的唯一线索，列表页页脚要显示它
    return { ...(await res.json()), cacheHeader: res.headers.get('X-Cache') || '—' };
  },

  stats() {
    return requestJSON(`${API}/stats`);
  },

  order(id) {
    return requestJSON(`${API}/orders/${id}`);
  },

  myOrders() {
    return requestJSON(`${API}/orders`);
  },

  // 一单多品。幂等键由调用方生成 —— 它必须和「这一次点击」绑定，
  // 放在这里生成的话，重试会换一个键，等于没有幂等。
  createOrder(items, idempotencyKey) {
    return requestJSON(`${API}/orders`, {
      method: 'POST',
      headers: { 'Idempotency-Key': idempotencyKey },
      body: JSON.stringify({ items }),
    });
  },

  login(email, password) {
    return requestJSON('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    });
  },

  register(email, password, displayName) {
    return requestJSON('/auth/register', {
      method: 'POST',
      body: JSON.stringify({ email, password, displayName }),
    });
  },

  async me() {
    // me 单独放开 401 处理：它的 401 是「没登录」，属于正常分支，
    // 不该触发全局的「登录已过期」提示。
    const token = getToken();
    if (!token) return null;
    const res = await fetch('/auth/me', {
      headers: { Authorization: `Bearer ${token}` },
      cache: 'no-store',
    });
    if (!res.ok) return null;
    return (await res.json()).user;
  },
};

export function newIdempotencyKey() {
  if (globalThis.crypto?.randomUUID) return crypto.randomUUID();
  return 'k-' + Date.now() + '-' + Math.random().toString(16).slice(2);
}

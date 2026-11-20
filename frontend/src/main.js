import './style.css';

const API = '/api';

const el = {
  products: document.getElementById('products'),
  toast: document.getElementById('toast'),
  pod: document.getElementById('pod'),
  cache: document.getElementById('cache'),
  queue: document.getElementById('queue'),
  who: document.getElementById('who'),
  authOpen: document.getElementById('auth-open'),
  logout: document.getElementById('logout'),
  authDialog: document.getElementById('auth-dialog'),
  authForm: document.getElementById('auth-form'),
  authTitle: document.getElementById('auth-title'),
  authError: document.getElementById('auth-error'),
  authSwitch: document.getElementById('auth-switch'),
  authSubmit: document.getElementById('auth-submit'),
  nameRow: document.getElementById('name-row'),
};

let lastCache = '—';
// token 放 localStorage：刷新页面不该掉登录态。
// 代价是 XSS 能偷走它 —— 真要抗这一点得上 httpOnly cookie + CSRF token，
// 这不是一个演示站该背的复杂度，但得知道边界在哪。
let token = localStorage.getItem('shop.token') || '';
let currentUser = null;

function toast(msg, kind = 'ok') {
  el.toast.textContent = msg;
  el.toast.dataset.kind = kind;
  el.toast.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { el.toast.hidden = true; }, 4000);
}

// 后端一律用「分」，前端只在显示时除以 100
function money(cents) {
  return '¥' + (Number(cents) / 100).toFixed(2);
}

function render(items, pod) {
  if (!items.length) {
    el.products.innerHTML = '<p class="loading">没有商品</p>';
    return;
  }
  el.products.innerHTML = items.map((p) => `
    <article class="card" data-id="${p.id}">
      <h2>${p.name}</h2>
      <p class="price">${money(p.priceCents)}</p>
      <p class="stock ${p.stock > 0 ? '' : 'out'}">
        ${p.stock > 0 ? `库存 ${p.stock}` : '已售罄'}
      </p>
      <button ${p.stock > 0 ? '' : 'disabled'} data-buy="${p.id}">
        立即下单
      </button>
    </article>
  `).join('');
  el.pod.textContent = 'api pod: ' + (pod || '—');
}

async function load() {
  try {
    const res = await fetch(`${API}/products`, { cache: 'no-store' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    lastCache = res.headers.get('X-Cache') || '—';
    const data = await res.json();
    render(data.items, data.pod);
    await loadStats();
  } catch (err) {
    el.products.innerHTML = `<p class="loading error">加载失败：${err.message}</p>`;
  }
}

async function loadStats() {
  try {
    const res = await fetch(`${API}/stats`, { cache: 'no-store' });
    if (!res.ok) return;
    const s = await res.json();
    el.cache.textContent = `cache: ${lastCache} (hit ${s.cache.hit} / miss ${s.cache.miss})`;
    el.queue.textContent = `queue: ${s.queue.depth}`;
  } catch { /* 统计挂了不影响下单 */ }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 订单状态的中文说明。状态机在服务端，前端只负责翻译。
const STATUS_TEXT = {
  CREATED: '已受理，等待支付',
  PAID: '支付成功，等待出库',
  SHIPPED: '已发货，运输中',
  DELIVERED: '已签收',
  CANCELLED: '已取消（支付失败）',
};

// 下单后轮询：CREATED 是 api 写的，PAID 是 worker 改的，
// SHIPPED / DELIVERED 是 Node 履约服务回写的 —— 一条链上四个进程，
// 前端能依次看到，就说明 outbox → RabbitMQ → worker/fulfillment 全通了。
async function pollOrder(id) {
  let last = '';
  // 40 次 × 1s ≈ 40s，够覆盖 支付 + 出库 + 干线 + 签收（演示里压到了秒级）
  for (let i = 0; i < 40; i++) {
    await sleep(1000);
    try {
      const res = await authFetch(`${API}/orders/${id}`);
      if (res.status === 404) {
        // 登录后才下的单，token 过期后会查不到 —— 这种情况提示重新登录
        toast(`订单 #${id} 查不到了，可能登录态已过期`, 'err');
        return;
      }
      if (!res.ok) continue;
      const o = await res.json();
      if (o.status === last) continue;
      last = o.status;

      const path = o.history.map((h) => h.toStatus).join(' → ');
      const ship = o.trackingNo ? `　运单 ${o.trackingNo}` : '';
      toast(`订单 #${o.id}：${STATUS_TEXT[o.status] || o.status}${ship}（${path}）`,
        o.status === 'CANCELLED' ? 'err' : 'ok');

      if (o.status === 'DELIVERED' || o.status === 'CANCELLED') {
        load();
        return;
      }
    } catch { /* 继续重试 */ }
  }
}

function newKey() {
  if (globalThis.crypto?.randomUUID) return crypto.randomUUID();
  return 'k-' + Date.now() + '-' + Math.random().toString(16).slice(2);
}

el.products.addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-buy]');
  if (!btn) return;

  const id = Number(btn.dataset.buy);
  btn.disabled = true;
  btn.textContent = '提交中…';

  try {
    const res = await authFetch(`${API}/orders`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        // 幂等键：网络抖动重试不会变成两张订单
        'Idempotency-Key': newKey(),
      },
      body: JSON.stringify({ productId: id, quantity: 1 }),
    });
    const data = await res.json();

    if (res.status === 201 || res.status === 200) {
      toast(`订单 #${data.id} 已受理，等待支付结果…`);
      pollOrder(data.id);
    } else {
      toast(data.error || `失败 HTTP ${res.status}`, 'err');
    }
  } catch (err) {
    toast('网络错误：' + err.message, 'err');
  } finally {
    btn.textContent = '立即下单';
    btn.disabled = false;
    load();
  }
});

load();
setInterval(loadStats, 5000);

// ---------------------------------------------------------------------------
// 账号：登录 / 注册 / 退出
//
// token 由 Go 账号服务签发，api 本地验签 —— 前端只负责存和带。
// 下单时带上它，订单就会归属到这个用户（不带也能下单，只是匿名）。
// ---------------------------------------------------------------------------

function authFetch(url, opts = {}) {
  const headers = new Headers(opts.headers || {});
  if (token) headers.set('Authorization', `Bearer ${token}`);
  return fetch(url, { ...opts, headers, cache: 'no-store' });
}

function setToken(next) {
  token = next || '';
  if (token) localStorage.setItem('shop.token', token);
  else localStorage.removeItem('shop.token');
}

function renderAccount() {
  const loggedIn = Boolean(currentUser);
  el.who.textContent = loggedIn ? (currentUser.displayName || currentUser.email) : '';
  el.who.hidden = !loggedIn;
  el.authOpen.hidden = loggedIn;
  el.logout.hidden = !loggedIn;
}

async function refreshAccount() {
  if (!token) {
    currentUser = null;
    renderAccount();
    return;
  }
  try {
    const res = await authFetch('/auth/me');
    if (res.ok) {
      currentUser = (await res.json()).user;
    } else {
      // token 过期或被吊销：直接清掉，别让它一直 401
      setToken('');
      currentUser = null;
    }
  } catch {
    // 网络问题不清 token，下次刷新再试
    currentUser = null;
  }
  renderAccount();
}

let authMode = 'login';

function openAuth(mode) {
  authMode = mode;
  el.authForm.reset();
  el.authError.hidden = true;
  el.authTitle.textContent = mode === 'login' ? '登录' : '注册';
  el.authSubmit.textContent = mode === 'login' ? '登录' : '注册并登录';
  el.authSwitch.textContent = mode === 'login' ? '没有账号？去注册' : '已有账号？去登录';
  el.nameRow.hidden = mode === 'login';
  el.authDialog.showModal();
}

el.authOpen.addEventListener('click', () => openAuth('login'));

el.authSwitch.addEventListener('click', () => {
  openAuth(authMode === 'login' ? 'register' : 'login');
});

el.authForm.addEventListener('submit', async (e) => {
  // 不用 method="dialog"：那样表单提交会直接关窗口，
  // 失败时用户看不到错误，得重开一次
  e.preventDefault();
  const fd = new FormData(el.authForm);
  const body = {
    email: String(fd.get('email') || '').trim(),
    password: String(fd.get('password') || ''),
  };
  if (authMode === 'register') {
    body.displayName = String(fd.get('displayName') || '').trim();
  }

  el.authSubmit.disabled = true;
  try {
    const path = authMode === 'login' ? '/auth/login' : '/auth/register';
    const res = await fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      el.authError.textContent = data.error || `失败 HTTP ${res.status}`;
      el.authError.hidden = false;
      return;
    }
    setToken(data.token);
    currentUser = data.user;
    renderAccount();
    el.authDialog.close();
    toast(`欢迎，${currentUser.displayName || currentUser.email}`);
  } catch (err) {
    el.authError.textContent = '网络错误：' + err.message;
    el.authError.hidden = false;
  } finally {
    el.authSubmit.disabled = false;
  }
});

el.logout.addEventListener('click', () => {
  // 纯前端退出：JWT 是无状态的，服务端没有会话可销毁。
  // 真要「立刻失效」就得有吊销列表，那是另一个量级的设计。
  setToken('');
  currentUser = null;
  renderAccount();
  toast('已退出登录');
});

refreshAccount();

// 深链：/#login 直接弹登录框。演示和截图时省得手点，
// 也方便把「请先登录」的链接直接指过来。
if (location.hash === '#login') openAuth('login');


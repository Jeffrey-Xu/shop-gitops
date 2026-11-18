import './style.css';

const API = '/api';

const el = {
  products: document.getElementById('products'),
  toast: document.getElementById('toast'),
  pod: document.getElementById('pod'),
  cache: document.getElementById('cache'),
  queue: document.getElementById('queue'),
};

let lastCache = '—';

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

// 下单后轮询订单状态：CREATED 是 API 写的，PAID 是 worker 异步改的。
// 前端能看到这个过渡，就说明消息链路真的通了。
async function pollOrder(id) {
  for (let i = 0; i < 12; i++) {
    await sleep(800);
    try {
      const res = await fetch(`${API}/orders/${id}`, { cache: 'no-store' });
      if (!res.ok) continue;
      const o = await res.json();
      const path = o.history.map((h) => h.toStatus).join(' → ');
      toast(`订单 #${o.id}：${o.status}（${path}）`, o.status === 'CANCELLED' ? 'err' : 'ok');
      if (o.status !== 'CREATED') {
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
    const res = await fetch(`${API}/orders`, {
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

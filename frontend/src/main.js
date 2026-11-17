import './style.css';

const API = '/api';

const el = {
  products: document.getElementById('products'),
  toast: document.getElementById('toast'),
  pod: document.getElementById('pod'),
};

function toast(msg, kind = 'ok') {
  el.toast.textContent = msg;
  el.toast.dataset.kind = kind;
  el.toast.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { el.toast.hidden = true; }, 3200);
}

function money(n) {
  return '¥' + Number(n).toFixed(2);
}

function render(items, pod) {
  if (!items.length) {
    el.products.innerHTML = '<p class="loading">没有商品</p>';
    return;
  }
  el.products.innerHTML = items.map((p) => `
    <article class="card" data-id="${p.id}">
      <h2>${p.name}</h2>
      <p class="price">${money(p.price)}</p>
      <p class="stock ${p.stock > 0 ? '' : 'out'}">
        ${p.stock > 0 ? `库存 ${p.stock}` : '已售罄'}
      </p>
      <button ${p.stock > 0 ? '' : 'disabled'} data-buy="${p.id}">
        加入购物车
      </button>
    </article>
  `).join('');
  el.pod.textContent = 'backend pod: ' + (pod || '—');
}

async function load() {
  try {
    const res = await fetch(`${API}/products`, { cache: 'no-store' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    render(data.items, data.pod);
  } catch (err) {
    el.products.innerHTML = `<p class="loading error">加载失败：${err.message}</p>`;
  }
}

el.products.addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-buy]');
  if (!btn) return;

  const id = Number(btn.dataset.buy);
  btn.disabled = true;
  btn.textContent = '提交中…';

  try {
    const res = await fetch(`${API}/cart`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ productId: id, quantity: 1 }),
    });
    const data = await res.json();

    if (res.status === 201) {
      toast(`下单成功 #${data.orderId ?? '—'}　金额 ${money(data.amount)}`);
    } else {
      toast(data.error || `失败 HTTP ${res.status}`, 'err');
    }
  } catch (err) {
    toast('网络错误：' + err.message, 'err');
  } finally {
    btn.textContent = '加入购物车';
    btn.disabled = false;
    load();
  }
});

load();

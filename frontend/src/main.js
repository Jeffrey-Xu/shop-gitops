import './style.css';
import { productArt } from './art.js';
import { api, getToken, setToken, setUnauthorizedHandler, newIdempotencyKey } from './api.js';
import { cart } from './cart.js';

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

const $ = (id) => document.getElementById(id);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 商品名和描述来自数据库，早晚会有人写个带 < 的名字进去。
// 所有插值都过这一层，别让商品文案变成 HTML。
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

// 整数不显示小数：¥599 比 ¥599.00 更像价签。
// 结算金额必须两位小数，那是钱。
function money(cents, forceDecimals = false) {
  const v = Number(cents || 0) / 100;
  return '¥' + (forceDecimals || !Number.isInteger(v) ? v.toFixed(2) : String(v));
}

// 价签里的数字部分（¥ 符号要单独排小一号，所以拆出来）
function priceNum(cents) {
  const v = Number(cents || 0) / 100;
  return Number.isInteger(v) ? String(v) : v.toFixed(2);
}

function stars(rating) {
  const full = Math.round(Number(rating) || 0);
  let out = '';
  for (let i = 1; i <= 5; i++) out += i <= full ? '★' : '<span>★</span>';
  return `<span class="stars" aria-label="评分 ${esc(rating)}">${out}</span>`;
}

function timeText(iso) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getMonth() + 1}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function toast(msg, kind = 'ok') {
  const el = $('toast');
  el.textContent = msg;
  el.dataset.kind = kind;
  el.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { el.hidden = true; }, 4200);
}

// ---------------------------------------------------------------------------
// 状态
// ---------------------------------------------------------------------------

const state = {
  products: [],
  byId: new Map(),
  categories: [],
  catalog: 0,
  total: 0,
  category: '',
  q: '',
  sort: '',
  user: null,
  cacheHeader: '—',
  ordersPoll: null,
};

const STATUS_TEXT = {
  CREATED: '已提交，等待支付',
  PAID: '支付成功，等待出库',
  SHIPPED: '已发货，运输中',
  DELIVERED: '已签收',
  CANCELLED: '已取消',
};

// 订单进度条的四段。CANCELLED 不走这条线，单独画。
const STEPS = [
  { key: 'CREATED', label: '已提交' },
  { key: 'PAID', label: '已支付' },
  { key: 'SHIPPED', label: '已发货' },
  { key: 'DELIVERED', label: '已签收' },
];

// ---------------------------------------------------------------------------
// 商品列表
// ---------------------------------------------------------------------------

function discountPct(p) {
  if (!p.originalPriceCents || p.originalPriceCents <= p.priceCents) return 0;
  return Math.round((1 - p.priceCents / p.originalPriceCents) * 100);
}

function cardHTML(p) {
  const off = discountPct(p);
  const soldOut = p.stock <= 0;
  const low = !soldOut && p.stock <= 10;

  let badge = '';
  if (soldOut) badge = '<span class="badge badge-out">已售罄</span>';
  else if (off >= 5) badge = `<span class="badge badge-sale">省 ${off}%</span>`;
  else if (p.sold >= 1500) badge = '<span class="badge badge-hot">热销</span>';

  const priceBlock = `
    <span class="price"><small>¥</small>${priceNum(p.priceCents)}</span>
    ${off ? `<s class="price-original">${money(p.originalPriceCents)}</s>` : ''}`;

  const stockHint = soldOut
    ? '<span class="stock-hint out">暂时缺货</span>'
    : low ? `<span class="stock-hint">仅剩 ${p.stock} 件</span>` : '';

  return `
    <article class="card" data-id="${p.id}">
      <button class="card-art" type="button" data-open="${p.id}" aria-label="查看 ${esc(p.name)}">
        ${badge}
        ${productArt(p.art, { class: 'art' })}
      </button>
      <div class="card-body">
        <span class="card-cat">${esc(p.category)}</span>
        <h3 class="card-title" data-open="${p.id}">${esc(p.name)}</h3>
        <div class="card-meta">
          ${stars(p.rating)}
          <span>${Number(p.rating).toFixed(1)}</span>
          <span>·</span>
          <span>已售 ${p.sold > 9999 ? (p.sold / 10000).toFixed(1) + '万' : p.sold}</span>
        </div>
        <div class="card-price-row">
          <div>${priceBlock}${stockHint ? '<br>' + stockHint : ''}</div>
          <button class="card-buy" type="button" data-add="${p.id}"
                  title="${soldOut ? '暂时缺货' : '加入购物车'}" ${soldOut ? 'disabled' : ''}>
            <svg viewBox="0 0 22 22" aria-hidden="true">
              <path d="M3 4h2.2l2 9.4h9.4l1.9-6.6H6.4" fill="none" stroke="currentColor"
                    stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" />
              <circle cx="9.4" cy="17.4" r="1.4" fill="currentColor" />
              <circle cx="15.6" cy="17.4" r="1.4" fill="currentColor" />
            </svg>
          </button>
        </div>
      </div>
    </article>`;
}

function renderCategoryNav() {
  const all = `<a href="#/" data-cat="" class="${state.category === '' ? 'active' : ''}">
      全部<em>${state.catalog}</em></a>`;
  const rest = state.categories.map((c) => `
    <a href="#/" data-cat="${esc(c.name)}" class="${state.category === c.name ? 'active' : ''}">
      ${esc(c.name)}<em>${c.count}</em></a>`).join('');
  $('catnav').innerHTML = all + rest;
}

function renderToolbar() {
  const active = [];
  if (state.category) {
    active.push(`<span class="filter-chip">${esc(state.category)}
      <button type="button" data-clear="category" aria-label="清除品类筛选">✕</button></span>`);
  }
  if (state.q) {
    active.push(`<span class="filter-chip">搜索：${esc(state.q)}
      <button type="button" data-clear="q" aria-label="清除搜索">✕</button></span>`);
  }

  return `
    <div class="toolbar">
      <span class="toolbar-info">共 <b>${state.total}</b> 件商品</span>
      ${active.join('')}
      <div class="toolbar-right">
        <label class="visually-hidden" for="sort-select">排序</label>
        <select class="select" id="sort-select">
          <option value="" ${state.sort === '' ? 'selected' : ''}>综合排序</option>
          <option value="sales" ${state.sort === 'sales' ? 'selected' : ''}>销量优先</option>
          <option value="price_asc" ${state.sort === 'price_asc' ? 'selected' : ''}>价格从低到高</option>
          <option value="price_desc" ${state.sort === 'price_desc' ? 'selected' : ''}>价格从高到低</option>
          <option value="rating" ${state.sort === 'rating' ? 'selected' : ''}>评分优先</option>
        </select>
      </div>
    </div>`;
}

function heroProduct() {
  // 挑「省得最多」的那件做主推，而不是「折扣率最高」的那件。
  // 按折扣率排会把 ¥89 的理线套装（省 31%）顶上首页大 banner，
  // 那是清仓位的逻辑，不是旗舰位的逻辑。同额优惠时价格高的优先。
  const withOff = state.products
    .filter((p) => discountPct(p) > 0 && p.stock > 0)
    .sort((a, b) => {
      const saveA = a.originalPriceCents - a.priceCents;
      const saveB = b.originalPriceCents - b.priceCents;
      return saveB - saveA || b.priceCents - a.priceCents;
    });
  return withOff[0] || state.products[0] || null;
}

function renderHome() {
  const hero = heroProduct();
  const heroBlock = hero ? `
    <section class="hero">
      <div>
        <span class="hero-eyebrow">本周主推 · ${esc(hero.category)}</span>
        <h1>桌面，<span>重新布置一次</span></h1>
        <p>${esc(hero.description || '从键盘到手感，从屏幕到光。挑一件每天都会用到的装备，让工作台顺手一点。')}</p>
        <div class="hero-actions">
          <button class="btn" type="button" data-open="${hero.id}">查看 ${esc(hero.name)}</button>
          <button class="btn btn-ghost" type="button" data-scroll="grid">浏览全部 ${state.catalog} 件</button>
        </div>
        <div class="hero-trust">
          <div><b>30 天</b>无理由退换</div>
          <div><b>满 ¥500</b>免运费</div>
          <div><b>${hero.sold}</b>人已购买</div>
        </div>
      </div>
      <div class="hero-showcase">
        ${productArt(hero.art, { class: 'art' })}
        <div class="hero-tag">
          <span>${esc(hero.name)}</span>
          <b><strong>${money(hero.priceCents)}</strong>
          ${hero.originalPriceCents ? `<s class="price-original">${money(hero.originalPriceCents)}</s>` : ''}</b>
        </div>
      </div>
    </section>` : '';

  const grid = state.products.length
    ? `<div class="grid" id="grid">${state.products.map(cardHTML).join('')}</div>`
    : `<div class="empty">
         <h3>没有找到匹配的商品</h3>
         <p>换个关键词，或者看看别的品类。</p>
         <button class="btn btn-ghost" type="button" data-reset>清除全部筛选</button>
       </div>`;

  $('view').innerHTML = heroBlock + renderToolbar() + grid;
  renderCategoryNav();
}

// 只取数据、不碰 DOM。
//
// 拆成两半是因为「我的订单」页也要商品数据：订单里只有 productId 和名称快照，
// 缩略图要靠商品表里的 art。订单页不该为了拿到它先把首页渲染一遍。
async function fetchCatalog() {
  const data = await api.products({ category: state.category, q: state.q, sort: state.sort });
  state.products = data.items || [];
  state.byId = new Map(state.products.map((p) => [p.id, p]));
  state.categories = data.categories || [];
  state.total = data.total ?? state.products.length;
  state.catalog = data.catalog ?? state.products.length;
  state.cacheHeader = data.cacheHeader;
  // pod 名来自响应头/响应体的 pod 字段，页脚用它证明「这次列表是哪个副本答的」
  $('stat-pod').textContent = `api pod ${data.pod || '—'}`;
  return data;
}

async function ensureCatalog() {
  if (state.products.length) return;
  try {
    await fetchCatalog();
  } catch { /* 订单页拿不到商品图也不该整页失败 */ }
}

async function loadProducts({ skeleton = true } = {}) {
  if (skeleton) {
    $('view').innerHTML = `
      <div class="skeleton-hero"></div>
      <div class="skeleton-grid">
        <div class="skeleton-card"></div><div class="skeleton-card"></div>
        <div class="skeleton-card"></div><div class="skeleton-card"></div>
      </div>`;
  }
  try {
    await fetchCatalog();
    renderHome();
  } catch (err) {
    $('view').innerHTML = `<div class="empty">
        <h3>商品加载失败</h3><p>${esc(err.message)}</p>
        <button class="btn btn-ghost" type="button" data-reload>重新加载</button>
      </div>`;
  }
}

// ---------------------------------------------------------------------------
// 商品详情
// ---------------------------------------------------------------------------

const dialog = $('product-dialog');

function specRows(p) {
  const rows = [];
  if (p.category) rows.push(['品类', p.category]);
  if (p.stock > 0) rows.push(['库存', `${p.stock} 件`]);
  else rows.push(['库存', '暂时缺货']);
  rows.push(['销量', `${p.sold} 件`]);
  rows.push(['发货', '付款后 7 天内（演示环境为秒级）']);
  rows.push(['保障', '30 天无理由 · 正品保障']);
  return rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join('');
}

function openProduct(id) {
  const p = state.byId.get(Number(id));
  if (!p) return;
  const soldOut = p.stock <= 0;
  const off = discountPct(p);
  const maxQty = Math.max(1, Math.min(p.stock, 99));

  dialog.innerHTML = `
    <button class="detail-close" type="button" data-close-dialog aria-label="关闭">✕</button>
    <div class="detail">
      <div class="detail-art">${productArt(p.art, { class: 'art' })}</div>
      <div class="detail-body">
        <span class="detail-cat">${esc(p.category)}</span>
        <h2>${esc(p.name)}</h2>
        <div class="card-meta">
          ${stars(p.rating)}<span>${Number(p.rating).toFixed(1)}</span>
          <span>·</span><span>已售 ${p.sold}</span>
        </div>
        <p class="detail-desc">${esc(p.description || '这件商品还没有写描述。')}</p>
        <div class="detail-price">
          <span class="price"><small>¥</small>${(p.priceCents / 100).toFixed(2)}</span>
          ${off ? `<s class="price-original">${money(p.originalPriceCents)}</s>
                   <span class="price-off">省 ${money(p.originalPriceCents - p.priceCents)}</span>` : ''}
        </div>
        <dl class="specs">${specRows(p)}</dl>
        <div class="buy-row">
          <div class="qty" data-qty-wrap>
            <button type="button" data-qty="-1" ${soldOut ? 'disabled' : ''}>−</button>
            <input type="number" value="1" min="1" max="${maxQty}" inputmode="numeric"
                   aria-label="购买数量" ${soldOut ? 'disabled' : ''}>
            <button type="button" data-qty="1" ${soldOut ? 'disabled' : ''}>+</button>
          </div>
          <button class="btn btn-block" type="button" data-detail-add="${p.id}" ${soldOut ? 'disabled' : ''}>
            ${soldOut ? '暂时缺货' : '加入购物车'}
          </button>
        </div>
        <button class="btn btn-price btn-block" type="button" data-detail-buy="${p.id}" ${soldOut ? 'disabled' : ''}>
          立即购买
        </button>
      </div>
    </div>`;
  dialog.showModal();
}

function currentQty() {
  const input = dialog.querySelector('[data-qty-wrap] input');
  return input ? Math.max(1, Math.min(99, Number(input.value) || 1)) : 1;
}

// ---------------------------------------------------------------------------
// 购物车
// ---------------------------------------------------------------------------

const drawer = $('cart-drawer');
const scrim = $('scrim');

function cartDetailed() {
  // 用商品表补全名称和价格。商品下架了就标出来，而不是静默删掉 ——
  // 用户得知道为什么金额变了。
  return cart.lines().map((l) => {
    const p = state.byId.get(l.id);
    return { ...l, product: p || null };
  });
}

const FREE_SHIPPING_CENTS = 50000;
const SHIPPING_CENTS = 1200;

function cartSummary(lines) {
  const subtotal = lines.reduce((n, l) => n + (l.product ? l.product.priceCents * l.qty : 0), 0);
  const shipping = subtotal === 0 || subtotal >= FREE_SHIPPING_CENTS ? 0 : SHIPPING_CENTS;
  return { subtotal, shipping, total: subtotal + shipping };
}

function renderCart() {
  const lines = cartDetailed();
  const count = cart.count();
  const badge = $('cart-count');
  badge.textContent = count > 99 ? '99+' : String(count);
  badge.hidden = count === 0;
  $('cart-title-count').textContent = count ? `（${count} 件）` : '';

  if (!lines.length) {
    $('cart-body').innerHTML = `
      <div class="empty" style="margin-top:20px;border:0;background:transparent">
        <h3>购物车还是空的</h3>
        <p>看看本周主推，或者按品类逛逛。</p>
        <button class="btn btn-ghost" type="button" data-close-cart>去逛逛</button>
      </div>`;
    $('cart-foot').hidden = true;
    return;
  }

  $('cart-body').innerHTML = lines.map((l) => {
    if (!l.product) {
      return `<div class="cart-line">
        <div class="cart-thumb"></div>
        <div class="cart-info"><span class="cart-name">商品已下架</span>
          <span class="cart-price">—</span></div>
        <div class="cart-line-actions">
          <button class="cart-remove" type="button" data-cart-remove="${l.id}">移除</button>
        </div></div>`;
    }
    return `<div class="cart-line">
      <div class="cart-thumb">${productArt(l.product.art, { class: 'art' })}</div>
      <div class="cart-info">
        <span class="cart-name">${esc(l.product.name)}</span>
        <span class="cart-price">${money(l.product.priceCents)}</span>
        <div class="qty qty-sm" data-qty-line="${l.id}">
          <button type="button" data-cart-qty="-1" data-id="${l.id}">−</button>
          <input type="number" value="${l.qty}" min="0" max="99" inputmode="numeric"
                 aria-label="${esc(l.product.name)} 数量">
          <button type="button" data-cart-qty="1" data-id="${l.id}">+</button>
        </div>
      </div>
      <div class="cart-line-actions">
        <button class="cart-remove" type="button" data-cart-remove="${l.id}">移除</button>
        <b>${money(l.product.priceCents * l.qty)}</b>
      </div>
    </div>`;
  }).join('');

  const sum = cartSummary(lines);
  $('cart-foot').hidden = false;
  $('cart-foot').innerHTML = `
    <div class="sum-row"><span>商品合计</span><span>${money(sum.subtotal, true)}</span></div>
    <div class="sum-row"><span>运费</span><span>${sum.shipping ? money(sum.shipping, true) : '免运费'}</span></div>
    ${sum.shipping ? `<div class="sum-row"><span>再买 ${money(FREE_SHIPPING_CENTS - sum.subtotal)} 免运费</span><span></span></div>` : ''}
    <div class="sum-row total"><span>应付</span><b>${money(sum.total, true)}</b></div>
    <button class="btn btn-price btn-block" type="button" data-checkout>去结算</button>
    <p class="drawer-note">下单后由支付网关扣款，履约服务自动生成运单</p>`;
}

function openCart() {
  renderCart();
  scrim.hidden = false;
  drawer.hidden = false;
  document.body.style.overflow = 'hidden';
}

function closeCart() {
  scrim.hidden = true;
  drawer.hidden = true;
  document.body.style.overflow = '';
}

// ---------------------------------------------------------------------------
// 结算
// ---------------------------------------------------------------------------

const checkoutDialog = $('checkout-dialog');

function stepClass(order, key) {
  if (order.status === 'CANCELLED') {
    return key === 'CREATED' ? 'done' : '';
  }
  const reached = order.history?.some((h) => h.toStatus === key);
  if (!reached) return '';
  // current 的橙色高亮是「正在进行」的信号。已签收是终态，
  // 还亮着会被读成「出事了」，所以 DELIVERED 一律按完成态画。
  return order.status === key && order.status !== 'DELIVERED' ? 'current' : 'done';
}

function timelineHTML(order) {
  if (order.status === 'CANCELLED') {
    return `<div class="timeline">
      <div class="step done"><span class="step-dot">✓</span>
        <div class="step-text"><b>已提交</b><small>${timeText(order.createdAt)}</small></div></div>
      <div class="step fail"><span class="step-dot">✕</span>
        <div class="step-text"><b>已取消</b>
          <small>${esc(order.history?.at(-1)?.note || '支付未成功')}</small></div></div>
    </div>`;
  }
  return `<div class="timeline">${STEPS.map((s) => {
    const cls = stepClass(order, s.key);
    const ev = order.history?.find((h) => h.toStatus === s.key);
    const extra = s.key === 'SHIPPED' && order.trackingNo
      ? `运单 ${esc(order.trackingNo)}` : '';
    return `<div class="step ${cls}">
      <span class="step-dot">${cls === 'done' ? '✓' : cls === 'current' ? '•' : ''}</span>
      <div class="step-text"><b>${s.label}</b>
        <small>${ev ? timeText(ev.createdAt) : '等待中'}${extra ? ' · ' + extra : ''}</small>
      </div></div>`;
  }).join('')}</div>`;
}

function checkoutBodyHTML(order, extraNote = '') {
  return `
    <div class="checkout">
      <h2>下单成功</h2>
      <p class="checkout-sub">订单 <b>#${order.id}</b> · ${STATUS_TEXT[order.status] || order.status}
        ${state.user ? '' : '· 当前未登录，登录后可在「我的订单」里查看'}</p>
      <div class="order-lines">
        ${(order.items || []).map((it) => `
          <div class="order-line"><span>${esc(it.name)} × ${it.quantity}</span>
            <span>${money(it.unitPriceCents * it.quantity, true)}</span></div>`).join('')}
        <div class="order-line" style="border-top:1px dashed var(--line);padding-top:10px">
          <span>合计</span><span><b>${money(order.totalCents, true)}</b></span></div>
      </div>
      <div id="timeline-slot">${timelineHTML(order)}</div>
      <p class="drawer-note" style="text-align:left">${esc(extraNote)}</p>
      <div class="dialog-actions" style="margin-top:18px">
        <button class="btn btn-ghost" type="button" data-close-dialog>继续购物</button>
        <a class="btn" href="#/orders" data-close-dialog>查看我的订单</a>
      </div>
    </div>`;
}

async function pollCheckout(orderId) {
  const slot = () => $('timeline-slot');
  // 60 次 × 1.2s ≈ 72s。演示环境整条链路压到了 40s 内，
  // 真发货是以天计的，那时候这里应该改成推送或定期刷新。
  for (let i = 0; i < 60; i++) {
    await sleep(1200);
    if (!checkoutDialog.open || !slot()) return; // 用户关了弹层就停
    try {
      const o = await api.order(orderId);
      const s = slot();
      if (!s) return;
      s.innerHTML = timelineHTML(o);
      const sub = checkoutDialog.querySelector('.checkout-sub');
      if (sub) {
        sub.innerHTML = `订单 <b>#${o.id}</b> · ${STATUS_TEXT[o.status] || o.status}`;
      }
      if (o.status === 'DELIVERED' || o.status === 'CANCELLED') {
        loadProducts({ skeleton: false });
        if (location.hash.startsWith('#/orders')) renderOrders();
        return;
      }
    } catch { /* 网络抖动就继续轮询 */ }
  }
}

async function checkout(lines, { clearCartAfter = false } = {}) {
  if (!lines.length) return;
  const items = lines.map((l) => ({ productId: l.id, quantity: l.qty }));

  checkoutDialog.innerHTML = `<div class="checkout">
      <h2>正在下单…</h2>
      <p class="checkout-sub">提交订单并等待支付网关结果</p>
      <div class="timeline"><div class="step current"><span class="step-dot">•</span>
        <div class="step-text"><b>提交中</b><small>请稍候</small></div></div></div>
    </div>`;
  checkoutDialog.showModal();

  try {
    // 幂等键和这次点击绑定：网络超时重试会命中同一订单，不会买两次
    const order = await api.createOrder(items, newIdempotencyKey());
    if (clearCartAfter) {
      cart.clear();
      renderCart();
      closeCart();
    }
    checkoutDialog.innerHTML = checkoutBodyHTML(order);
    pollCheckout(order.id);
    loadProducts({ skeleton: false });
  } catch (err) {
    checkoutDialog.innerHTML = `<div class="checkout">
        <h2>下单失败</h2>
        <p class="checkout-sub">${esc(err.message)}</p>
        <div class="dialog-actions" style="margin-top:16px">
          <button class="btn btn-ghost" type="button" data-close-dialog>关闭</button>
          <button class="btn" type="button" data-retry-checkout>重试</button>
        </div>
      </div>`;
    const retry = checkoutDialog.querySelector('[data-retry-checkout]');
    retry?.addEventListener('click', () => checkout(lines, { clearCartAfter }), { once: true });
  }
}

// ---------------------------------------------------------------------------
// 我的订单
// ---------------------------------------------------------------------------

function orderCardHTML(o) {
  const cancelled = o.status === 'CANCELLED';
  const steps = STEPS.map((s, i) => {
    const reached = o.history?.some((h) => h.toStatus === s.key);
    const isCurrent = o.status === s.key && o.status !== 'DELIVERED';
    const cls = cancelled
      ? (i === 0 ? 'done' : '')
      : reached ? (isCurrent ? 'done current' : 'done') : '';
    const ev = o.history?.find((h) => h.toStatus === s.key);
    return `<div class="ostep ${cls}"><span class="dot"></span>
      <b>${s.label}</b><small>${ev ? timeText(ev.createdAt) : ''}</small></div>`;
  }).join('');

  return `
    <article class="order-card">
      <div class="order-card-head">
        <b>订单 #${o.id}</b>
        <span>${timeText(o.createdAt)}</span>
        <span class="status-pill status-${o.status}">${STATUS_TEXT[o.status] || o.status}</span>
        <span class="spacer"></span>
        ${o.trackingNo ? `<span class="tracking">${esc(o.trackingNo)}</span>` : ''}
      </div>
      <div class="order-items">
        ${(o.items || []).map((it) => {
          const p = state.byId.get(it.productId);
          return `<div class="order-item">
            <div class="cart-thumb">${p ? productArt(p.art, { class: 'art' }) : ''}</div>
            <div>
              <div class="order-item-name">${esc(it.name)}</div>
              <div class="order-item-meta">${money(it.unitPriceCents)} × ${it.quantity}</div>
            </div>
            <div class="order-item-meta">${money(it.unitPriceCents * it.quantity, true)}</div>
          </div>`;
        }).join('')}
      </div>
      <div class="order-side">
        <div class="order-total"><small>实付</small>${money(o.totalCents, true)}</div>
        <a class="btn btn-ghost btn-sm" href="#/">再次购买</a>
      </div>
      <div class="order-steps">${steps}</div>
      ${cancelled ? `<p class="drawer-note" style="grid-column:1/-1;text-align:left">
        取消原因：${esc(o.history?.at(-1)?.note || '支付未成功')}</p>` : ''}
    </article>`;
}

let ordersLoading = false;

async function renderOrders() {
  if (!getToken()) {
    $('view').innerHTML = `
      <div class="section-head"><h1>我的订单</h1></div>
      <div class="empty">
        <h3>登录后才能查看订单</h3>
        <p>未登录时下的单不会归属到账号，登录后可查到自己名下的订单。</p>
        <button class="btn" type="button" data-login>立即登录</button>
      </div>`;
    return;
  }
  if (ordersLoading) return;
  ordersLoading = true;

  if (!$('view').querySelector('.order-card')) {
    $('view').innerHTML = `<div class="section-head">
        <h1>我的订单</h1><p>正在加载…</p></div>`;
  }

  try {
    const data = await api.myOrders();
    const items = data.items || [];

    // head 里的计数必须在拼字符串时就定下来。
    // 先插一个「正在加载…」再回头改 DOM，第二次赋值 innerHTML 会把它覆盖回去。
    const head = `<div class="section-head">
        <h1>我的订单</h1>
        <p>${items.length ? `共 ${items.length} 笔` : ''}</p>
      </div>`;

    const body = items.length
      ? items.map(orderCardHTML).join('')
      : `<div class="empty">
           <h3>还没有订单</h3>
           <p>挑一件桌面装备，下单后这里会实时显示发货进度。</p>
           <a class="btn" href="#/">去逛逛</a>
         </div>`;

    $('view').innerHTML = head + body;

    // 有未完成的订单就自动刷新：发货是后台进程推进的，
    // 不刷新的话用户会以为卡住了。
    const pending = items.some((o) => !['DELIVERED', 'CANCELLED'].includes(o.status));
    clearTimeout(state.ordersPoll);
    if (pending) state.ordersPoll = setTimeout(renderOrders, 4000);
  } catch (err) {
    $('view').innerHTML = `<div class="section-head"><h1>我的订单</h1></div>
      <div class="empty">
        <h3>订单加载失败</h3><p>${esc(err.message)}</p>
        <button class="btn btn-ghost" type="button" data-reload-orders>重试</button>
      </div>`;
    if (err.status === 401) renderAccount();
  } finally {
    ordersLoading = false;
  }
}

// ---------------------------------------------------------------------------
// 账号
// ---------------------------------------------------------------------------

function renderAccount() {
  const loggedIn = Boolean(state.user);
  $('who').textContent = loggedIn ? (state.user.displayName || state.user.email) : '';
  $('who').hidden = !loggedIn;
  $('auth-open').hidden = loggedIn;
  $('logout').hidden = !loggedIn;
}

async function refreshAccount() {
  state.user = await api.me();
  if (!state.user && getToken()) setToken('');
  renderAccount();
  return state.user;
}

const authDialog = $('auth-dialog');
let authMode = 'login';

function openAuth(mode = 'login') {
  authMode = mode;
  $('auth-form').reset();
  $('auth-error').hidden = true;
  $('auth-title').textContent = mode === 'login' ? '登录' : '注册';
  $('auth-sub').textContent = mode === 'login'
    ? '登录后订单会记在「我的订单」里'
    : '注册即自动登录，密码至少 8 位';
  $('auth-submit').textContent = mode === 'login' ? '登录' : '注册并登录';
  $('auth-switch').textContent = mode === 'login' ? '没有账号？去注册' : '已有账号？去登录';
  $('name-row').hidden = mode === 'login';
  authDialog.showModal();
}

function authError(msg) {
  const el = $('auth-error');
  el.textContent = msg;
  el.hidden = !msg;
}

$('auth-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const email = String(fd.get('email') || '').trim();
  const password = String(fd.get('password') || '');
  const displayName = String(fd.get('displayName') || '').trim();

  $('auth-submit').disabled = true;
  authError('');
  try {
    const data = authMode === 'login'
      ? await api.login(email, password)
      : await api.register(email, password, displayName);
    setToken(data.token);
    state.user = data.user;
    renderAccount();
    authDialog.close();
    toast(`欢迎，${state.user.displayName || state.user.email}`);
    // 订单页在登录态变化后要立刻重取
    if (location.hash.startsWith('#/orders')) renderOrders();
    else loadProducts({ skeleton: false });
  } catch (err) {
    authError(err.message);
  } finally {
    $('auth-submit').disabled = false;
  }
});

// ---------------------------------------------------------------------------
// 事件绑定
// ---------------------------------------------------------------------------

$('cart-btn').addEventListener('click', openCart);
$('cart-close').addEventListener('click', closeCart);
scrim.addEventListener('click', closeCart);
$('nav-orders').addEventListener('click', () => { closeCart(); });

$('auth-open').addEventListener('click', () => openAuth('login'));
$('auth-switch').addEventListener('click', () => openAuth(authMode === 'login' ? 'register' : 'login'));
$('logout').addEventListener('click', () => {
  // 纯前端退出：JWT 无状态，服务端没有会话可销毁。
  setToken('');
  state.user = null;
  renderAccount();
  toast('已退出登录');
  if (location.hash.startsWith('#/orders')) renderOrders();
});

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && !drawer.hidden) closeCart();
});

// 筛选条件变了：先回首页，再决定谁去请求。
// 如果当前就在首页，直接重取；不在首页的话 hash 会变，交给 hashchange 处理 ——
// 两边都调一次 fetch 的话，用户点一下会打两次接口。
function applyFilter() {
  if (location.hash && location.hash !== '#/') location.hash = '#/';
  else loadProducts();
}

$('search-form').addEventListener('submit', (e) => {
  e.preventDefault();
  state.q = $('search-input').value.trim();
  applyFilter();
});

// 品类导航
$('catnav').addEventListener('click', (e) => {
  const a = e.target.closest('a[data-cat]');
  if (!a) return;
  e.preventDefault();
  const next = a.dataset.cat;
  if (next === state.category) return;
  state.category = next;
  applyFilter();
});

// 主视图里的所有交互集中在一个委托上。
// 每个卡片各挂一个监听会在重新渲染时泄漏，委托没这个问题。
$('view').addEventListener('click', (e) => {
  const open = e.target.closest('[data-open]');
  if (open) { openProduct(open.dataset.open); return; }

  const add = e.target.closest('[data-add]');
  if (add) {
    cart.add(Number(add.dataset.add), 1);
    const p = state.byId.get(Number(add.dataset.add));
    toast(`已加入购物车：${p ? p.name : ''}`);
    renderCart();
    return;
  }

  const scroll = e.target.closest('[data-scroll]');
  if (scroll) {
    $('grid')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    return;
  }

  const clear = e.target.closest('[data-clear]');
  if (clear) {
    if (clear.dataset.clear === 'category') state.category = '';
    if (clear.dataset.clear === 'q') { state.q = ''; $('search-input').value = ''; }
    loadProducts();
    return;
  }

  if (e.target.closest('[data-reset]')) {
    state.category = '';
    state.q = '';
    $('search-input').value = '';
    loadProducts();
    return;
  }

  if (e.target.closest('[data-reload]')) { loadProducts(); return; }
  if (e.target.closest('[data-reload-orders]')) { renderOrders(); return; }
  if (e.target.closest('[data-login]')) { openAuth('login'); return; }
});

$('view').addEventListener('change', (e) => {
  if (e.target.id === 'sort-select') {
    state.sort = e.target.value;
    loadProducts({ skeleton: false });
  }
});

// 商品详情弹层
dialog.addEventListener('click', (e) => {
  if (e.target.closest('[data-close-dialog]') || e.target === dialog) {
    dialog.close();
    return;
  }
  const qtyBtn = e.target.closest('[data-qty]');
  if (qtyBtn) {
    const input = dialog.querySelector('[data-qty-wrap] input');
    const max = Number(input.max) || 99;
    const next = Math.max(1, Math.min(max, (Number(input.value) || 1) + Number(qtyBtn.dataset.qty)));
    input.value = String(next);
    return;
  }
  const add = e.target.closest('[data-detail-add]');
  if (add) {
    const q = currentQty();
    cart.add(Number(add.dataset.detailAdd), q);
    toast(`已加入购物车 × ${q}`);
    renderCart();
    dialog.close();
    return;
  }
  const buy = e.target.closest('[data-detail-buy]');
  if (buy) {
    const q = currentQty();
    dialog.close();
    checkout([{ id: Number(buy.dataset.detailBuy), qty: q }]);
  }
});

// 购物车抽屉
drawer.addEventListener('click', (e) => {
  if (e.target.closest('[data-close-cart]')) { closeCart(); return; }

  const qtyBtn = e.target.closest('[data-cart-qty]');
  if (qtyBtn) {
    const id = Number(qtyBtn.dataset.id);
    const line = cart.lines().find((l) => l.id === id);
    if (line) cart.setQty(id, line.qty + Number(qtyBtn.dataset.cartQty));
    renderCart();
    return;
  }

  const rm = e.target.closest('[data-cart-remove]');
  if (rm) {
    cart.remove(Number(rm.dataset.cartRemove));
    renderCart();
    return;
  }

  if (e.target.closest('[data-checkout]')) {
    const lines = cartDetailed().filter((l) => l.product);
    if (!lines.length) { toast('购物车里没有可结算的商品', 'err'); return; }
    checkout(lines, { clearCartAfter: true });
  }
});

drawer.addEventListener('change', (e) => {
  const input = e.target.closest('[data-qty-line] input');
  if (!input) return;
  const id = Number(input.closest('[data-qty-line]').dataset.qtyLine);
  cart.setQty(id, Number(input.value) || 0);
  renderCart();
});

checkoutDialog.addEventListener('click', (e) => {
  if (e.target.closest('[data-close-dialog]') || e.target === checkoutDialog) {
    checkoutDialog.close();
  }
});

// ---------------------------------------------------------------------------
// 页脚运行时指标
// ---------------------------------------------------------------------------

async function loadStats() {
  try {
    const s = await api.stats();
    $('stat-cache').textContent = `cache ${state.cacheHeader} · hit ${s.cache.hit} / miss ${s.cache.miss}`;
    $('stat-queue').textContent = `queue ${s.queue.depth}`;
  } catch { /* 统计挂了不影响购物 */ }
}

// ---------------------------------------------------------------------------
// 路由
//
// hash 路由足够用：这个站只有两个页面，引入 router 库不划算。
// 真正的收益是「刷新不丢页面」，所以任何状态下都要能从 URL 复原。
// ---------------------------------------------------------------------------

function route() {
  clearTimeout(state.ordersPoll);
  const hash = location.hash || '#/';
  const isOrders = hash.startsWith('#/orders');

  $('nav-orders').classList.toggle('active', isOrders);
  $('search-input').value = state.q;
  window.scrollTo({ top: 0 });

  if (isOrders) {
    renderCategoryNav();
    // 订单页的缩略图依赖商品表，但不能为了它把首页先渲染一遍
    ensureCatalog().then(renderOrders);
  } else {
    loadProducts();
  }
  loadStats();
}

window.addEventListener('hashchange', route);

// ---------------------------------------------------------------------------
// 启动
// ---------------------------------------------------------------------------

setUnauthorizedHandler(() => {
  state.user = null;
  renderAccount();
});

cart.subscribe(() => renderCart());

(async function boot() {
  await refreshAccount();
  route();
  // 深链：/#login 直接弹登录框，方便把「请先登录」的链接指过来
  if (location.hash === '#login') openAuth('login');
  setInterval(loadStats, 6000);
})();

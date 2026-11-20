// 购物车。
//
// 存在 localStorage 里：刷新不掉、关标签页也不掉，符合用户对购物车的预期。
// 只存 {id, qty}，不存价格 —— 价格必须每次从后端拿，
// 否则改价之后购物车里还是旧价，结算时金额对不上会很尴尬。

const KEY = 'shop.cart.v2';

function read() {
  try {
    const raw = JSON.parse(localStorage.getItem(KEY) || '[]');
    if (!Array.isArray(raw)) return [];
    return raw
      .filter((l) => l && Number.isFinite(l.id) && Number.isFinite(l.qty) && l.qty > 0)
      .map((l) => ({ id: Number(l.id), qty: Math.min(Math.floor(l.qty), 99) }));
  } catch {
    return [];
  }
}

let lines = read();
const listeners = new Set();

function commit() {
  localStorage.setItem(KEY, JSON.stringify(lines));
  listeners.forEach((fn) => fn(lines));
}

export const cart = {
  lines() {
    return lines;
  },

  count() {
    return lines.reduce((n, l) => n + l.qty, 0);
  },

  /** @param {(lines: {id:number,qty:number}[]) => void} fn */
  subscribe(fn) {
    listeners.add(fn);
    return () => listeners.delete(fn);
  },

  add(id, qty = 1) {
    const line = lines.find((l) => l.id === id);
    if (line) line.qty = Math.min(line.qty + qty, 99);
    else lines.push({ id, qty: Math.min(Math.max(1, qty), 99) });
    commit();
  },

  setQty(id, qty) {
    const n = Math.max(0, Math.min(Math.floor(qty), 99));
    const i = lines.findIndex((l) => l.id === id);
    if (i < 0) return;
    if (n === 0) lines.splice(i, 1);
    else lines[i].qty = n;
    commit();
  },

  remove(id) {
    lines = lines.filter((l) => l.id !== id);
    commit();
  },

  clear() {
    lines = [];
    commit();
  },
};

// 多标签页同步：另一个标签页改了购物车，这边要跟着变。
// storage 事件不会在本标签页触发，所以不会和上面的 commit 打架。
globalThis.addEventListener?.('storage', (e) => {
  if (e.key !== KEY) return;
  lines = read();
  listeners.forEach((fn) => fn(lines));
});

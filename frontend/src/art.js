// 商品插画：每个 art key 一段手绘 SVG。
//
// 为什么不用图片文件：这个集群经常拉不到外部镜像/图床，真实店铺那种照片
// 一旦加载失败就是一片灰块。矢量图跟 CSS 一起进包，离线也永远画得出来，
// 而且换主题色不用重新出图。
//
// 统一规则（"棚拍"感）：
//   - 背景一律近白，带一点从左上来的暖调光晕
//   - 主体用同色系深浅两色，暗部在右、亮部在左
//   - 底部一条极淡的椭圆投影，制造"放在台面上"的落地感
// 有了统一规则，十几张不同形状的图放在一个网格里才像同一个品牌拍的。

let seq = 0;

// 每个商品的主色。深浅两档用于主体，accent 用于唯一的点缀（指示灯、logo）。
const PALETTE = {
  keyboard: { dark: '#39414d', light: '#5c6775', accent: '#2f7f6f' },
  mouse: { dark: '#4a5058', light: '#767d86', accent: '#2f7f6f' },
  numpad: { dark: '#3d4550', light: '#626c79', accent: '#c9a227' },
  monitor: { dark: '#2c333c', light: '#4c5663', accent: '#3b7ea1' },
  monitor2: { dark: '#2f3640', light: '#525c68', accent: '#3b7ea1' },
  headphones: { dark: '#333a44', light: '#5b6470', accent: '#b4553f' },
  speaker: { dark: '#6b5540', light: '#9c7f60', accent: '#3f5f4a' },
  mic: { dark: '#3a4048', light: '#606974', accent: '#a8493a' },
  chair: { dark: '#3f4650', light: '#6a727d', accent: '#2f7f6f' },
  desk: { dark: '#7a5c42', light: '#a98460', accent: '#39414d' },
  stand: { dark: '#46505c', light: '#6f7a87', accent: '#8b939d' },
  charger: { dark: '#3b424b', light: '#646e7a', accent: '#2f7f6f' },
  lamp: { dark: '#3c434c', light: '#666f7a', accent: '#e0a83c' },
  camera: { dark: '#31373f', light: '#565f6a', accent: '#3b7ea1' },
  cable: { dark: '#4b525b', light: '#7a828c', accent: '#b4553f' },
  lampbar: { dark: '#343b44', light: '#5b646f', accent: '#e0a83c' },
  box: { dark: '#5d6570', light: '#8b939d', accent: '#2f7f6f' },
};

// 形状表。每个函数拿到 (uid, p)，返回主体部分的 SVG 片段。
// 统一按 400×300 的画布坐标写，方便对齐"台面线"（y≈248）。
const SHAPES = {
  keyboard(uid, p) {
    let keys = '';
    // 5 行 × 13 列键帽，最后一排留长空格
    for (let r = 0; r < 5; r++) {
      const w = r === 4 ? 13 : 13;
      for (let c = 0; c < w; c++) {
        if (r === 4 && c > 3 && c < 9) continue; // 空格位置
        keys += `<rect x="${86 + c * 17.6}" y="${124 + r * 15.4}" width="14.4" height="12.6"
                   rx="2.6" fill="#f2f1ed" opacity="${0.82 + (r % 2) * 0.06}"/>`;
      }
    }
    keys += `<rect x="158" y="${124 + 4 * 15.4}" width="88" height="12.6" rx="2.6" fill="#f2f1ed" opacity="0.9"/>`;
    return `
      <rect x="72" y="112" width="256" height="104" rx="12" fill="url(#body${uid})"/>
      <rect x="72" y="112" width="256" height="104" rx="12" fill="none" stroke="#000" stroke-opacity="0.08"/>
      <rect x="80" y="120" width="240" height="88" rx="7" fill="${p.dark}" opacity="0.55"/>
      ${keys}
      <rect x="296" y="196" width="18" height="4" rx="2" fill="${p.accent}" opacity="0.9"/>`;
  },

  mouse(uid, p) {
    return `
      <path d="M200 96 c26 0 40 24 40 58 v26 c0 26-18 40-40 40 s-40-14-40-40 v-26 c0-34 14-58 40-58 z"
            fill="url(#body${uid})" stroke="#000" stroke-opacity="0.08"/>
      <path d="M200 96 v58" stroke="#000" stroke-opacity="0.12" stroke-width="1.5"/>
      <rect x="196" y="118" width="8" height="20" rx="4" fill="${p.accent}" opacity="0.85"/>
      <path d="M168 160 c0-30 12-52 32-58" stroke="#fff" stroke-opacity="0.28" stroke-width="3" fill="none" stroke-linecap="round"/>`;
  },

  numpad(uid, p) {
    let keys = '';
    for (let r = 0; r < 5; r++) {
      for (let c = 0; c < 4; c++) {
        keys += `<rect x="${150 + c * 26}" y="${106 + r * 28}" width="21" height="23" rx="4"
                   fill="#f2f1ed" opacity="0.86"/>`;
      }
    }
    return `
      <rect x="138" y="94" width="124" height="164" rx="14" fill="url(#body${uid})"/>
      <rect x="138" y="94" width="124" height="164" rx="14" fill="none" stroke="#000" stroke-opacity="0.08"/>
      ${keys}
      <circle cx="248" cy="104" r="9" fill="${p.accent}"/>`;
  },

  monitor(uid, p) {
    return `
      <rect x="76" y="82" width="248" height="150" rx="8" fill="url(#body${uid})"/>
      <rect x="84" y="90" width="232" height="134" rx="4" fill="url(#screen${uid})"/>
      <path d="M186 232 h28 l10 22 h-48 z" fill="${p.light}"/>
      <rect x="150" y="252" width="100" height="8" rx="4" fill="${p.dark}"/>
      <rect x="96" y="102" width="60" height="5" rx="2.5" fill="#fff" opacity="0.35"/>
      <rect x="96" y="114" width="96" height="5" rx="2.5" fill="#fff" opacity="0.2"/>
      <circle cx="316" cy="226" r="3" fill="${p.accent}"/>`;
  },

  monitor2(uid, p) {
    return `
      <rect x="144" y="62" width="112" height="196" rx="8" fill="url(#body${uid})"/>
      <rect x="152" y="70" width="96" height="180" rx="3" fill="url(#screen${uid})"/>
      <path d="M186 258 h28 l8 18 h-44 z" fill="${p.light}"/>
      <rect x="156" y="274" width="88" height="7" rx="3.5" fill="${p.dark}"/>
      <rect x="162" y="82" width="34" height="4" rx="2" fill="#fff" opacity="0.35"/>
      <rect x="162" y="92" width="62" height="4" rx="2" fill="#fff" opacity="0.18"/>`;
  },

  headphones(uid, p) {
    return `
      <path d="M120 186 v-24 c0-44 36-78 80-78 s80 34 80 78 v24" fill="none"
            stroke="url(#body${uid})" stroke-width="17" stroke-linecap="round"/>
      <rect x="100" y="172" width="42" height="76" rx="18" fill="url(#body${uid})" stroke="#000" stroke-opacity="0.08"/>
      <rect x="258" y="172" width="42" height="76" rx="18" fill="url(#body${uid})" stroke="#000" stroke-opacity="0.08"/>
      <rect x="108" y="182" width="12" height="56" rx="6" fill="${p.accent}" opacity="0.55"/>
      <rect x="280" y="182" width="12" height="56" rx="6" fill="${p.accent}" opacity="0.55"/>
      <circle cx="121" cy="240" r="4" fill="${p.accent}"/>`;
  },

  speaker(uid, p) {
    return `
      <rect x="132" y="84" width="136" height="176" rx="14" fill="url(#body${uid})"/>
      <rect x="132" y="84" width="136" height="176" rx="14" fill="none" stroke="#000" stroke-opacity="0.1"/>
      <circle cx="200" cy="146" r="34" fill="${p.dark}" opacity="0.5"/>
      <circle cx="200" cy="146" r="24" fill="url(#body${uid})"/>
      <circle cx="200" cy="214" r="18" fill="${p.dark}" opacity="0.5"/>
      <circle cx="200" cy="214" r="11" fill="url(#body${uid})"/>
      <rect x="150" y="98" width="100" height="3" rx="1.5" fill="#fff" opacity="0.2"/>`;
  },

  mic(uid, p) {
    return `
      <rect x="180" y="70" width="40" height="96" rx="20" fill="url(#body${uid})"/>
      <rect x="186" y="78" width="8" height="72" rx="4" fill="#fff" opacity="0.12"/>
      <path d="M156 132 v10 c0 26 20 44 44 44 s44-18 44-44 v-10" fill="none"
            stroke="${p.light}" stroke-width="5" stroke-linecap="round"/>
      <rect x="196" y="186" width="8" height="30" fill="${p.light}"/>
      <path d="M162 216 h76" stroke="url(#body${uid})" stroke-width="9" stroke-linecap="round"/>
      <circle cx="200" cy="94" r="4.5" fill="${p.accent}"/>`;
  },

  chair(uid, p) {
    return `
      <rect x="156" y="56" width="88" height="108" rx="14" fill="url(#body${uid})"/>
      <path d="M166 74 h68 M166 92 h68 M166 110 h68 M166 128 h68" stroke="#fff"
            stroke-opacity="0.13" stroke-width="2.5"/>
      <rect x="132" y="168" width="136" height="24" rx="11" fill="url(#body${uid})"/>
      <rect x="194" y="192" width="12" height="24" fill="${p.light}"/>
      <circle cx="200" cy="218" r="7" fill="${p.dark}"/>
      <path d="M200 218 L231 230 M200 218 L216 246 M200 218 L200 250 M200 218 L184 246 M200 218 L169 230"
            stroke="${p.dark}" stroke-width="5" stroke-linecap="round" fill="none"/>
      <circle cx="231" cy="230" r="4" fill="${p.accent}" opacity="0.9"/>
      <circle cx="216" cy="246" r="4" fill="${p.accent}" opacity="0.9"/>
      <circle cx="200" cy="250" r="4" fill="${p.accent}" opacity="0.9"/>
      <circle cx="184" cy="246" r="4" fill="${p.accent}" opacity="0.9"/>
      <circle cx="169" cy="230" r="4" fill="${p.accent}" opacity="0.9"/>`;
  },

  desk(uid, p) {
    return `
      <rect x="66" y="120" width="268" height="14" rx="5" fill="url(#body${uid})"/>
      <rect x="86" y="134" width="10" height="94" fill="${p.dark}" opacity="0.85"/>
      <rect x="304" y="134" width="10" height="94" fill="${p.dark}" opacity="0.85"/>
      <rect x="86" y="216" width="42" height="8" rx="4" fill="${p.dark}"/>
      <rect x="272" y="216" width="42" height="8" rx="4" fill="${p.dark}"/>
      <rect x="264" y="104" width="40" height="14" rx="4" fill="${p.light}"/>
      <rect x="272" y="108" width="10" height="4" rx="2" fill="${p.accent}"/>
      <rect x="278" y="126" width="56" height="5" rx="2.5" fill="#fff" opacity="0.25"/>`;
  },

  stand(uid, p) {
    // 画的是「显示器挂在悬臂上」而不是一根光杆：光杆的支架在缩略图里
    // 完全看不出是什么，挂一块屏幕才能一眼认出来。
    return `
      <rect x="118" y="238" width="112" height="11" rx="5" fill="${p.dark}"/>
      <rect x="166" y="96" width="13" height="146" fill="url(#body${uid})"/>
      <rect x="152" y="86" width="42" height="13" rx="6" fill="${p.light}"/>
      <rect x="186" y="140" width="26" height="28" rx="9" fill="${p.light}"/>
      <rect x="206" y="150" width="66" height="9" rx="4.5" fill="url(#body${uid})"/>
      <rect x="262" y="120" width="22" height="66" rx="9" fill="${p.light}"/>
      <rect x="282" y="92" width="84" height="112" rx="9" fill="url(#body${uid})"/>
      <rect x="290" y="100" width="68" height="92" rx="4" fill="url(#screen${uid})"/>
      <circle cx="199" cy="154" r="4.5" fill="${p.accent}"/>`;
  },

  charger(uid, p) {
    return `
      <rect x="100" y="170" width="200" height="62" rx="20" fill="url(#body${uid})"/>
      <rect x="100" y="170" width="200" height="62" rx="20" fill="none" stroke="#000" stroke-opacity="0.08"/>
      <rect x="146" y="84" width="58" height="98" rx="11" fill="${p.dark}"/>
      <rect x="153" y="92" width="44" height="78" rx="5" fill="#1d2733"/>
      <circle cx="252" cy="140" r="24" fill="${p.dark}"/>
      <circle cx="252" cy="140" r="15" fill="${p.light}"/>
      <rect x="234" y="196" width="38" height="15" rx="7.5" fill="${p.light}"/>
      <circle cx="118" cy="201" r="4" fill="${p.accent}"/>`;
  },

  lamp(uid, p) {
    return `
      <ellipse cx="200" cy="252" rx="46" ry="9" fill="${p.dark}"/>
      <rect x="192" y="150" width="10" height="96" fill="${p.light}"/>
      <path d="M197 152 c0-40 34-60 62-44" stroke="${p.light}" stroke-width="10" fill="none" stroke-linecap="round"/>
      <path d="M252 100 l44 30 -18 22 -44-30 z" fill="url(#body${uid})"/>
      <path d="M240 148 l-26 62 34 12 24-56 z" fill="${p.accent}" opacity="0.16"/>
      <circle cx="262" cy="116" r="5" fill="#fff" opacity="0.85"/>`;
  },

  camera(uid, p) {
    return `
      <rect x="118" y="106" width="164" height="98" rx="20" fill="url(#body${uid})"/>
      <circle cx="200" cy="155" r="34" fill="${p.dark}"/>
      <circle cx="200" cy="155" r="22" fill="#1b2028"/>
      <circle cx="200" cy="155" r="10" fill="${p.accent}" opacity="0.45"/>
      <circle cx="188" cy="144" r="6" fill="#fff" opacity="0.5"/>
      <rect x="176" y="204" width="48" height="38" rx="6" fill="${p.light}"/>
      <rect x="160" y="238" width="80" height="8" rx="4" fill="${p.dark}"/>
      <circle cx="262" cy="124" r="4" fill="${p.accent}"/>`;
  },

  cable(uid, p) {
    return `
      <path d="M104 200 c0-46 40-56 62-28 c22 28 44 28 66 0 c22-28 62-18 62 28"
            fill="none" stroke="url(#body${uid})" stroke-width="13" stroke-linecap="round"/>
      <rect x="88" y="192" width="30" height="20" rx="6" fill="${p.dark}"/>
      <rect x="292" y="192" width="30" height="20" rx="6" fill="${p.dark}"/>
      <rect x="96" y="196" width="12" height="12" rx="2" fill="${p.accent}" opacity="0.85"/>
      <rect x="300" y="196" width="12" height="12" rx="2" fill="${p.accent}" opacity="0.85"/>
      <rect x="150" y="140" width="16" height="34" rx="5" fill="${p.light}"/>
      <rect x="228" y="140" width="16" height="34" rx="5" fill="${p.light}"/>`;
  },

  lampbar(uid, p) {
    return `
      <rect x="82" y="122" width="236" height="22" rx="11" fill="url(#body${uid})"/>
      <rect x="94" y="138" width="212" height="5" rx="2.5" fill="#fff" opacity="0.75"/>
      <path d="M194 144 l-14 60 h40 l-10-60 z" fill="${p.accent}" opacity="0.14"/>
      <rect x="176" y="104" width="48" height="20" rx="7" fill="${p.light}"/>
      <circle cx="200" cy="114" r="4" fill="${p.accent}"/>
      <rect x="120" y="150" width="160" height="4" rx="2" fill="#fff" opacity="0.12"/>`;
  },

  box(uid, p) {
    return `
      <path d="M200 96 l84 30 v86 l-84 30 -84-30 v-86 z" fill="url(#body${uid})"/>
      <path d="M116 126 l84 30 84-30" fill="none" stroke="#fff" stroke-opacity="0.25" stroke-width="2"/>
      <path d="M200 156 v86" stroke="#fff" stroke-opacity="0.18" stroke-width="2"/>
      <rect x="192" y="96" width="16" height="18" fill="${p.accent}" opacity="0.7"/>`;
  },
};

/**
 * 生成一个商品插画的 SVG 字符串。
 * @param {string} art  art key（未知的会退回 box）
 * @param {{width?:number, height?:number, class?:string}} opts
 */
export function productArt(art, opts = {}) {
  const key = SHAPES[art] ? art : 'box';
  const p = PALETTE[key] || PALETTE.box;
  const uid = ++seq;
  const width = opts.width || 400;
  const height = opts.height || 300;

  return `<svg viewBox="0 0 400 300" width="${width}" height="${height}"
      class="art ${opts.class || ''}" role="img" aria-hidden="true"
      preserveAspectRatio="xMidYMid meet">
    <defs>
      <linearGradient id="bg${uid}" x1="0" y1="0" x2="0.6" y2="1">
        <stop offset="0" stop-color="#fdfcfa"/>
        <stop offset="1" stop-color="#eeece6"/>
      </linearGradient>
      <linearGradient id="body${uid}" x1="0.1" y1="0" x2="0.9" y2="1">
        <stop offset="0" stop-color="${p.light}"/>
        <stop offset="1" stop-color="${p.dark}"/>
      </linearGradient>
      <linearGradient id="screen${uid}" x1="0" y1="0" x2="1" y2="1">
        <stop offset="0" stop-color="#1d2733"/>
        <stop offset="0.55" stop-color="#2f4257"/>
        <stop offset="1" stop-color="#16202b"/>
      </linearGradient>
    </defs>
    <rect width="400" height="300" fill="url(#bg${uid})"/>
    <ellipse cx="200" cy="256" rx="118" ry="14" fill="#2b2b28" opacity="0.07"/>
    <ellipse cx="200" cy="254" rx="80" ry="8" fill="#2b2b28" opacity="0.06"/>
    ${SHAPES[key](uid, p)}
  </svg>`;
}

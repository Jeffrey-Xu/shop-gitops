-- 0003_catalog.sql
-- 把 products 从「三个名字加一个价格」扩成能撑起一个商城目录的样子。
--
-- 新增字段都是「展示用元数据」：价格和库存仍然是唯一有业务含义的两列，
-- 其余只是让商品卡有东西可画。刻意不引入 SKU / 变体 / 多图，
-- 那会连带把 order_items 和库存扣减的模型改掉，和这次的目的不成比例。

ALTER TABLE products ADD COLUMN description          TEXT    NOT NULL DEFAULT '';
ALTER TABLE products ADD COLUMN category             TEXT    NOT NULL DEFAULT '配件';
-- art 是前端的插画标识，不是图片 URL。
-- 用标识而不是 URL，是为了让前端能按同一套矢量风格渲染，
-- 同时避免商品数据里混进构建产物的路径（换 CDN 就要改数据）。
ALTER TABLE products ADD COLUMN art                  TEXT    NOT NULL DEFAULT 'box';
ALTER TABLE products ADD COLUMN rating               NUMERIC(2,1) NOT NULL DEFAULT 5.0;
ALTER TABLE products ADD COLUMN sold                 INT     NOT NULL DEFAULT 0;
-- 划线价。NULL 表示没有促销，前端就不画那条线。
ALTER TABLE products ADD COLUMN original_price_cents BIGINT;

-- 品类筛选是列表页的主路径，加个索引；排序仍在内存里做（目录只有十几条，
-- 为它建 (category, price_cents) 的组合索引是过度设计）。
CREATE INDEX products_category_idx ON products (category, id);

-- ---------------------------------------------------------------------------
-- 一、已有的三个商品补齐元数据
-- 名字加了型号后缀：真实店铺的标题几乎都带型号，纯品类名看着像原型。
-- stock 不动 —— 它已经被真实下单扣减过，改它就是造假数据。
-- ---------------------------------------------------------------------------
UPDATE products SET
    name                 = '机械键盘 K87 客制化版',
    description          = '热插拔轴座，Gasket 结构，支持三模连接。附赠一套 PBT 键帽。',
    category             = '输入设备',
    art                  = 'keyboard',
    rating               = 4.8,
    sold                 = 1284,
    original_price_cents = 79900
 WHERE id = 1 AND name = '机械键盘';

UPDATE products SET
    name                 = '27 寸 4K 显示器 U27',
    description          = 'IPS 面板，98% DCI-P3，出厂校色 ΔE<2。支持 90W 反向供电，一根线接笔记本。',
    category             = '显示设备',
    art                  = 'monitor',
    rating               = 4.7,
    sold                 = 862,
    original_price_cents = 229900
 WHERE id = 2 AND name = '27 寸显示器';

UPDATE products SET
    name                 = '人体工学椅 Ergo Pro',
    description          = '四级联动腰托，网面透气，12 年质保。扶手四向可调，可躺 135°。',
    category             = '桌面家具',
    art                  = 'chair',
    rating               = 4.9,
    sold                 = 431,
    original_price_cents = NULL
 WHERE id = 3 AND name = '人体工学椅';

-- ---------------------------------------------------------------------------
-- 二、扩充目录到 16 个商品 / 5 个品类
--
-- 用 WHERE NOT EXISTS 判重而不是裸 INSERT：这个迁移只跑一次（schema_migrations
-- 有记录），但万一有人在已经手工插过数据的环境上重放，裸 INSERT 会造出重复商品。
-- 判重键用 name —— 这个目录里名字是唯一的。
-- ---------------------------------------------------------------------------
INSERT INTO products (name, price_cents, stock, description, category, art, rating, sold, original_price_cents)
SELECT v.name, v.price_cents, v.stock, v.description, v.category, v.art, v.rating, v.sold, v.original_price_cents
  FROM (VALUES
    ('无线鼠标 M3 静音版',      32900,  64, '2.4G + 蓝牙双模，静音微动，一节电池用一年。三台设备一键切换。',        '输入设备', 'mouse',     4.6,  2310, 39900::BIGINT),
    ('24 寸 2K 竖屏显示器 P24', 129900, 22, '16:10 竖屏，适合看文档和代码。旋转升降支架，Type-C 65W 供电。',        '显示设备', 'monitor2',   4.5,  547,  NULL),
    ('头戴式降噪耳机 H900',     149900, 18, '混合主动降噪 42dB，60 小时续航，LDAC 高清传输。可折叠收纳。',        '音频设备', 'headphones', 4.8,  1673, 179900::BIGINT),
    ('桌面蓝牙音箱 S40',         69900, 31, '双单元 2.0 声道，木质箱体，支持 AUX 与蓝牙 5.3。',                  '音频设备', 'speaker',    4.4,  903,  NULL),
    ('USB 电容麦克风 V2',        89900, 26, '心形指向电容头，内置防震架，免驱即插即用。附防喷罩。',                '音频设备', 'mic',        4.7,  688,  NULL),
    ('铝合金显示器支架 A1',      45900, 45, '承重 9kg，气弹簧悬臂，支持 17–32 寸。理线槽藏在臂内。',                '配件',     'stand',      4.6,  1204, 52900::BIGINT),
    ('电动升降桌 1.4m',         259900,  9, '双电机，承重 100kg，四档记忆高度。桌面 140×70cm 实木贴面。',          '桌面家具', 'desk',       4.8,  316, 299900::BIGINT),
    ('机械数字小键盘 N21',       25900, 58, '21 键独立数字区，可编程旋钮，铝合金外壳。',                          '输入设备', 'numpad',     4.3,  742,  NULL),
    ('三合一无线充电座',         19900, 77, '手机 + 耳机 + 手表同时充，15W 快充，折叠后只有巴掌大。',              '配件',     'charger',    4.5,  1988, 24900::BIGINT),
    ('桌面护眼灯 L5',            39900, 40, '无频闪，Ra97 高显色，色温 2700–6500K 无级调节。',                    '配件',     'lamp',       4.7,  1520,  NULL),
    ('4K 高清摄像头 C8',         59900, 35, '索尼传感器，4K30 / 1080P60，内置双麦降噪，物理挡片。',                '配件',     'camera',     4.4,  615,  NULL),
    ('理线收纳套装',              8900, 120, '魔术贴 + 理线管 + 理线夹，桌面线材一次收干净。',                     '配件',     'cable',      4.2,  3105, 12900::BIGINT),
    ('显示器挂灯 L1',            34900, 28, '非对称光路不反光，USB 供电，旋钮无级调光。',                          '配件',     'lampbar',    4.6,  1122,  NULL)
  ) AS v(name, price_cents, stock, description, category, art, rating, sold, original_price_cents)
 WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.name = v.name);

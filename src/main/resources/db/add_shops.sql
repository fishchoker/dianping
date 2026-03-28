-- 新增店铺数据SQL脚本
-- 使用说明：
-- 1. 可以直接在数据库管理工具中执行此脚本
-- 2. 或者通过命令行导入：mysql -u root -p hmdp < add_shops.sql

-- 店铺表结构参考：
-- id: 主键，自增
-- name: 商铺名称
-- type_id: 商铺类型ID（1-美食，2-KTV，3-丽人·美发，4-健身运动，5-按摩·足疗，6-美容SPA，7-亲子游乐，8-酒吧，9-轰趴馆，10-美睫·美甲）
-- images: 商铺图片，多个图片以','隔开
-- area: 商圈，例如陆家嘴
-- address: 地址
-- x: 经度
-- y: 纬度
-- avg_price: 均价，取整数（单位：元）
-- sold: 销量
-- comments: 评论数量
-- score: 评分，1~5分，乘10保存，避免小数（例如：45表示4.5分）
-- open_hours: 营业时间，例如 10:00-22:00

-- 示例：新增美食店铺
INSERT INTO `tb_shop` (`name`, `type_id`, `images`, `area`, `address`, `x`, `y`, `avg_price`, `sold`, `comments`, `score`, `open_hours`) 
VALUES 
('新开川菜馆', 1, 'https://img.meituan.net/msmerchant/example1.jpg,https://img.meituan.net/msmerchant/example2.jpg', '拱宸桥', '拱墅区金华路100号', 120.150000, 30.320000, 80, 100, 50, 45, '11:00-22:00'),
('日式料理店', 1, 'https://img.meituan.net/msmerchant/example3.jpg', '大关', '上塘路200号', 120.151000, 30.321000, 150, 200, 120, 48, '10:00-21:00'),
('韩式烤肉店', 1, 'https://img.meituan.net/msmerchant/example4.jpg', '运河上街', '台州路50号', 120.152000, 30.322000, 120, 150, 80, 46, '11:30-22:30');

-- 示例：新增KTV店铺
INSERT INTO `tb_shop` (`name`, `type_id`, `images`, `area`, `address`, `x`, `y`, `avg_price`, `sold`, `comments`, `score`, `open_hours`) 
VALUES 
('星空KTV', 2, 'https://p0.meituan.net/joymerchant/example5.jpg', '北部新城', '杭行路300号', 120.153000, 30.323000, 80, 300, 150, 47, '14:00-02:00'),
('麦乐迪KTV', 2, 'https://p0.meituan.net/joymerchant/example6.jpg', '远洋乐堤港', '丽水路100号', 120.154000, 30.324000, 100, 250, 120, 46, '12:00-24:00');

-- 注意：
-- 1. 请根据实际情况修改店铺信息
-- 2. 图片URL需要是有效的图片链接
-- 3. 经度(x)和纬度(y)需要使用实际的地理坐标
-- 4. type_id必须是对应的店铺类型ID（可以在tb_shop_type表中查看）
-- 5. score字段是评分乘以10，例如4.5分应存储为45

-- 查询所有店铺类型
-- SELECT * FROM tb_shop_type ORDER BY sort;

-- 查询已插入的店铺
-- SELECT * FROM tb_shop ORDER BY id DESC LIMIT 10;


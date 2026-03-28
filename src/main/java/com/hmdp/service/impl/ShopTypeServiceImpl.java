package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import com.hmdp.utils.RedisConstants;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
	@Resource
	private StringRedisTemplate stringRedisTemplate;
	@Override
    public Result queryTypeList() {
		//从redis查商户缓存 id作为店铺的key
	String shopTypeJson=stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOPTYPE_KEY);
	if(StrUtil.isNotBlank(shopTypeJson))
	{
		//存在 直接返回
		List<ShopType> typeList=JSONUtil.toList(shopTypeJson,ShopType.class);
	}
	//查数据库 若不存在返回错误
	List<ShopType> typeList= query().orderByAsc("sort").list();
	if(typeList==null)
	{
		return Result.fail("店铺类型不存在！");
	}
	//数据库中存在 写入redis
	stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOPTYPE_KEY,JSONUtil.toJsonStr(typeList));
	return Result.ok(typeList);
	
	}
}

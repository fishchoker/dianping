package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisConstants;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements IVoucherService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
	@Resource
	private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryVoucherOfShop(Long shopId) {
        // 查询优惠券信息
        List<Voucher> vouchers = getBaseMapper().queryVoucherOfShop(shopId);
        // 返回结果
        return Result.ok(vouchers);
    }

    @Override
    @Transactional
    public void addSeckillVoucher(Voucher voucher) {
        // 保存优惠券
        save(voucher);
        // 保存秒杀信息
        SeckillVoucher seckillVoucher = new SeckillVoucher();
        seckillVoucher.setVoucherId(voucher.getId());
        seckillVoucher.setStock(voucher.getStock());
        seckillVoucher.setBeginTime(voucher.getBeginTime());
        seckillVoucher.setEndTime(voucher.getEndTime());
        seckillVoucherService.save(seckillVoucher);
        /*
         * 保存到redis 注入redisTemplate
         */
        //保存库存 可以永久保存手动删除
        stringRedisTemplate.opsForValue().set(RedisConstants.SECKILL_STOCK_KEY +voucher.getId(), 
        		voucher.getStock().toString());   
        
    }

    @Override
    @Transactional
    public Result updateVoucher(Voucher voucher) {
        if (voucher.getId() == null) {
            return Result.fail("优惠券ID不能为空！");
        }
        
        // 先查询原有优惠券信息，判断是否为秒杀券
        Voucher oldVoucher = getById(voucher.getId());
        if (oldVoucher == null) {
            return Result.fail("优惠券不存在！");
        }
        
        // 更新数据库
        boolean success = updateById(voucher);
        if (!success) {
            return Result.fail("更新失败！");
        }
        
        // 如果是秒杀券，需要同步更新Redis中的库存信息
        // 判断类型：如果新数据有type则用新数据，否则用原数据
        Integer voucherType = voucher.getType() != null ? voucher.getType() : oldVoucher.getType();
        if (voucherType != null && voucherType == 1) {
            // 查询最新的秒杀券信息（从数据库获取最新值）
            SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucher.getId());
            if (seckillVoucher != null) {
                // 如果提供了库存信息，更新秒杀券表
                if (voucher.getStock() != null) {
                    seckillVoucher.setStock(voucher.getStock());
                    seckillVoucherService.updateById(seckillVoucher);
                    // 重新查询确保获取最新值
                    seckillVoucher = seckillVoucherService.getById(voucher.getId());
                }
                // 同步更新Redis中的库存（使用数据库中的最新值）
                String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucher.getId();
                stringRedisTemplate.opsForValue().set(stockKey, 
                    String.valueOf(seckillVoucher.getStock()));
            }
        }
        
        return Result.ok();
    }

    @Override
    public Result refreshVoucherCache(Long voucherId) {
        if (voucherId == null) {
            return Result.fail("优惠券ID不能为空！");
        }
        
        // 查询优惠券信息
        Voucher voucher = getById(voucherId);
        if (voucher == null) {
            return Result.fail("优惠券不存在！");
        }
        
        // 如果是秒杀券，刷新Redis中的库存信息
        if (voucher.getType() != null && voucher.getType() == 1) {
            SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
            if (seckillVoucher != null) {
                // 从数据库重新加载库存到Redis
                String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherId;
                stringRedisTemplate.opsForValue().set(stockKey, 
                    String.valueOf(seckillVoucher.getStock()));
                return Result.ok("秒杀券库存缓存已刷新！");
            } else {
                return Result.fail("秒杀券信息不存在！");
            }
        } else {
            return Result.ok("普通券无需刷新Redis缓存！");
        }
    }
}

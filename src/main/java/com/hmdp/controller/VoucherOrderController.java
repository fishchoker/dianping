package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;

import javax.annotation.Resource;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {
    @Resource
    private IVoucherOrderService  voucherOrderService;
    
    /**
     * 秒杀券抢购
     * @param voucherId 优惠券id
     * @return 订单id
     */
    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        //return Result.fail("功能未完成");
    	return voucherOrderService.seckillVoucher(voucherId);
    }
    
    /**
     * 普通券购买
     * @param voucherId 优惠券id
     * @return 订单id
     */
    @PostMapping("buy/{id}")
    public Result buyVoucher(@PathVariable("id") Long voucherId) {
    	return voucherOrderService.buyVoucher(voucherId);
    }
}

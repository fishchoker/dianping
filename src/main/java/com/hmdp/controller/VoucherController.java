package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.entity.Voucher;
import com.hmdp.service.IVoucherService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher")
public class VoucherController {

    @Resource
    private IVoucherService voucherService;

    /**
     * 新增普通券
     * @param voucher 优惠券信息
     * @return 优惠券id
     */
    @PostMapping
    public Result addVoucher(@RequestBody Voucher voucher) {
        voucherService.save(voucher);
        return Result.ok(voucher.getId());
    }

    /**
     * 新增秒杀券
     * @param voucher 优惠券信息，包含秒杀信息
     * @return 优惠券id
     */
    @PostMapping("seckill")
    public Result addSeckillVoucher(@RequestBody Voucher voucher) {
        voucherService.addSeckillVoucher(voucher);
        return Result.ok(voucher.getId());
    }

    /**
     * 查询店铺的优惠券列表
     * @param shopId 店铺id
     * @return 优惠券列表
     */
    @GetMapping("/list/{shopId}")
    public Result queryVoucherOfShop(@PathVariable("shopId") Long shopId) {
       return voucherService.queryVoucherOfShop(shopId);
    }

    /**
     * 更新优惠券信息（同时更新Redis缓存）
     * @param voucher 优惠券信息
     * @return 更新结果
     */
    @PutMapping
    public Result updateVoucher(@RequestBody Voucher voucher) {
        return voucherService.updateVoucher(voucher);
    }

    /**
     * 刷新优惠券的Redis缓存（从数据库重新加载到Redis）
     * @param voucherId 优惠券ID
     * @return 刷新结果
     */
    @PostMapping("/refresh/{voucherId}")
    public Result refreshVoucherCache(@PathVariable("voucherId") Long voucherId) {
        return voucherService.refreshVoucherCache(voucherId);
    }
}

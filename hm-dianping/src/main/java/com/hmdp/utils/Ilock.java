package com.hmdp.utils;

public interface Ilock {
	/**
     * 尝试获取锁
     *
     * @param timeoutSec 超时自动释放锁
     * @return 是否成功获取锁 true成功 false失败
     */
    boolean tryLock(long timeoutSec);

    /**
     * 释放锁
     */
    void unLock();
}

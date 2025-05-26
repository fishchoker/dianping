package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.entity.User;
import com.hmdp.dto.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IUserService extends IService<User> {
	/*
	 * sendcode
	 * 校验手机号
	 * 生成验证码
	 * 保存到redis
	 * 发送验证码
	 */
	public Result sendCode(String phone, HttpSession session);

	public Result login(LoginFormDTO loginForm, HttpSession session);
	
}

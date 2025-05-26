package com.hmdp.utils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.lang.Nullable;
import org.springframework.web.servlet.HandlerInterceptor;


import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;

public class RefreshTokenInterceptor implements HandlerInterceptor{
	
	//不可以使用@Resource自动构造
	private StringRedisTemplate stringRedisTemplate;
	
	//需要编写一个构造函数
	public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate)
	{
		this.stringRedisTemplate=stringRedisTemplate;
	}
	//前置拦截

	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
			throws Exception {
		/*
		 * //获取session HttpSession session =request.getSession();
		 */
		//获取token 在请求头中
		String token=request.getHeader("authorization");
		if(StrUtil.isBlankIfStr(token))//判断是否为空
		{
			//不拦截
			return true;
		}
		//获取用户 需要取出全部字段
		//Object user = session.getAttribute("user");
		Map<Object,Object> userMap=stringRedisTemplate.opsForHash().entries(RedisConstants.LOGIN_USER_KEY+token);
		//判断用户是否存在
		if(userMap.isEmpty())
		{
			return true;
		}
		//把用户对象从哈希转回来
		UserDTO userDTO =BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);//第三个参数 不忽略异常
		//存在就保存到threadlocal
		UserHolder.saveUser(userDTO);
		//刷新有效期
		stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY+token,RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);
		//放行
		return true;
	}
	//销毁避免内存泄漏
	public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler,
			@Nullable Exception ex) throws Exception {
		UserHolder.removeUser();
	}
}


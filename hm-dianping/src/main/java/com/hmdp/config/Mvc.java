package com.hmdp.config;

import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.hmdp.utils.LoginInterceptor;
import com.hmdp.utils.RefreshTokenInterceptor;
@Configuration
public class Mvc implements WebMvcConfigurer{
	@Resource
	private StringRedisTemplate stringRedisTemplate;
	//添加拦截器
	public void addInterceptors(InterceptorRegistry registry)
	{
		registry.addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate))
		.addPathPatterns("/**").order(0);//默认拦所有请求
		registry.addInterceptor(new LoginInterceptor())
		.excludePathPatterns(
				"/user/code",
				"/user/login",
				"/blog/hot",
				"/shop/**",//放行shop打头的所有请求
				"/shop-type/**",
				"/upload/**",//测试用
				"/voucher/**"
				).order(1);
		//决定哪些要放行 （登陆之前+不需要登录就能看的）

	}

}

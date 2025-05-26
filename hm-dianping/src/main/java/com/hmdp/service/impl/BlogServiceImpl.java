package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
	@Resource
	private StringRedisTemplate stringRedisTemplate;
    
	@Override
	public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        //records.forEach(this::queryBlogUser);
        records.forEach(blog ->{
        	this.queryBlogUser(blog);
        	this.isBlogLiked(blog);
        });
        return Result.ok(records);
	}

	@Override
	public Result queryBlogById(Long id) {
		// TODO 查询blog
		Blog blog=getById(id);
		if(blog== null) {
			return Result.fail("笔记不存在！");
		}
		queryBlogUser(blog);
		//查询blog是否被 当前登录的用户 点赞
		isBlogLiked(blog);
		return Result.ok(blog);
	}
	
	private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
	}

	@Override
	public Result likeBlog(Long id) {
		//获取登录用户
		Long userId = UserHolder.getUser().getId();
		//判断当前用户是否已经点赞
		String key=RedisConstants.BLOG_LIKED_KEY +id;
		//能取到分数则说明值必然存在
		Double score=stringRedisTemplate.opsForZSet().score(key, userId.toString());
		if(score==null) {
			//若未点赞
			//数据库点赞数+1
			Boolean isSuccess=update().setSql("liked =liked +1").eq("id", id).update();
			//保存用户信息到redis中
			if(isSuccess) {
				stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
			}
		}else {
			Boolean isSuccess=update().setSql("liked =liked -1").eq("id", id).update();
		//取消点赞
			if(isSuccess) {
				stringRedisTemplate.opsForZSet().remove(key,userId.toString());
			}
		}
		return Result.ok();
	}
	private void isBlogLiked(Blog blog) {
		//首页如果未登录
		UserDTO user=UserHolder.getUser();
		if(user ==null)//用户未登录 不查
		{
			return;
		}
		Long userId = user.getId();
		String key=RedisConstants.BLOG_LIKED_KEY +blog.getId();
		Double score=stringRedisTemplate.opsForZSet().score(key, userId.toString());
		blog.setIsLike(score !=null);
	}

	@Override
	public Result queryBlogLikes(Long id) {
		// TODO 实现查询top5的点赞用户 zrange key 0 4
		String key=RedisConstants.BLOG_LIKED_KEY +id;
		Set<String> top5=stringRedisTemplate.opsForZSet().range(key,0,4);
		if(top5==null||top5.isEmpty()) {
			return Result.ok(Collections.emptyList());
		}
		//解析出用户ID
		List<Long> id5=top5.stream().map(Long::valueOf).collect(Collectors.toList());
		String str=StrUtil.join(",", id5);
		//根据用户id做查询 需要orderby
		List<UserDTO> userDTOs=userService.query().in("id",id5).last("ORDER BY FIELD (id,"+ str+")").list()
				.stream()
				.map(user->BeanUtil.copyProperties(user,UserDTO.class))
				.collect(Collectors.toList());//UserDTO
		
		return Result.ok(userDTOs);
	}
}

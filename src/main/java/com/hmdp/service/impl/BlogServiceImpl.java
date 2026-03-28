package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import jodd.util.StringUtil;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
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
	@Resource
	private IFollowService followService;
    
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

	@Override
	public Result saveBlog(Blog blog) {
		// TODO Auto-generated method stub
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        if (blog.getShopId() == null) {  // 假设用户没有店铺的标识是 shopId 为 null
            return Result.fail("请关联店铺！");
        }
        // 保存探店博文
        boolean isSuccess=save(blog);
        if(!isSuccess) {
        	return Result.fail("新增笔记失败！");
        }
        //查询笔记作者的粉丝 follow表 follow_user_id=ID
        List<Follow> followers=followService.query().eq("follow_user_id",user.getId()).list();
        //推送给粉丝
        for(Follow follower:followers) {
        	Long userId=follower.getUserId();
        	//推送到sorted set key为粉丝ID
        	String key=RedisConstants.FEED_KEY+userId;
        	stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        	
        }
        // 返回id
        return Result.ok(blog.getId());
	}

	@Override
	public Result queryBlogOfFollow(Long max, Integer offset) {
		// TODO 实现滚动分页查询
		//找到收件箱
		Long userId = UserHolder.getUser().getId();
		String key=RedisConstants.FEED_KEY+userId;
		//ZREVRANGEBYSCORE key max min LIMIT offset count
		Set<ZSetOperations.TypedTuple<String>> typedTuples=stringRedisTemplate.opsForZSet()
		.reverseRangeByScoreWithScores(key,0,max,offset,3);
		if(typedTuples==null||typedTuples.isEmpty())
		{
			return Result.ok();
		}
		//解析 blogID min时间戳 offset
		List<Long> ids=new ArrayList<>(typedTuples.size());//必须显式指定大小
		long minTime=0;
		int offSet=1;
		for(ZSetOperations.TypedTuple<String> tuple:typedTuples) {
			ids.add(Long.valueOf(tuple.getValue()));
			long time=tuple.getScore().longValue();
			if(time==minTime) {
				offSet++;//时间戳 不断覆盖 最后就是最后一个元素的时间戳（最小）
			}else {
				minTime=time;//后取到的时间戳一定更小 覆盖
				offSet=1;
			}
		}
		String idStr=StringUtil.join(ids,",");
		List<Blog> blogs=query().in("id", ids).last("ORDER BY FIELD(id,"+idStr+")").list();
		for(Blog blog:blogs) {
			queryBlogUser(blog);
			//查询blog是否被 当前登录的用户 点赞
			isBlogLiked(blog);
		}
		//关联用户
		ScrollResult r=new ScrollResult();
		r.setList(blogs);
		r.setOffset(offSet);
		r.setMinTime(minTime);
		//封装并返回
		return Result.ok(r);
	}
}

package com.hmdp.dto;

import lombok.Data;

import java.util.List;

@Data
public class ScrollResult {
    private List<?> list;//通用 不显式指定
    private Long minTime;
    private Integer offset;
}

package com.hailin.shrine.job.common.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@AllArgsConstructor
@Getter
@ToString(of = "plainText")
public class JobExecutionEventThrowable {

    private final Throwable throwable;

    private String plainText;
}

package com.hufudb.openhufu.benchmark.annotation;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;

@Aspect
public class HandlingTimeAspect {
  @Pointcut("(execution(* *(..)) && @annotation(HandlingTime))")
  private void operationLog() {
  }

  @Around("operationLog()")
  public Object handlingTimeAround(ProceedingJoinPoint joinPoint) {
    try {
      long startTime = System.currentTimeMillis();
      Object proceed = joinPoint.proceed();
      System.out.println("Execution time：" + (System.currentTimeMillis() - startTime));
      return proceed;
    } catch (Throwable throwable) {
      throwable.printStackTrace();
    }
    return null;
  }
}

/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.reactive.utils;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableList;


public class ScheduledExceutor implements ScheduledExecutorService {
    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(0);
    static {
        EXECUTOR.setKeepAliveTime(60, TimeUnit.SECONDS);
        EXECUTOR.allowCoreThreadTimeOut(true);
    }

    public static ScheduledExecutorService common() {
        return new ScheduledExceutor(EXECUTOR);
    }

    
    private final ScheduledExecutorService executor;
        
    public ScheduledExceutor(ScheduledExecutorService executor) {
        this.executor = executor;
    }
    
    
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }
    
    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }
    
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return executor.invokeAll(tasks);
    }
    
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return executor.invokeAll(tasks, timeout, unit);
    }
    
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return executor.invokeAny(tasks);
    }
    
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return executor.invokeAny(tasks, timeout, unit);
    }
    
    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return executor.schedule(callable, delay, unit);
    }
    
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return executor.schedule(command, delay, unit);
    }
    
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return executor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }
    
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,long initialDelay, long delay, TimeUnit unit) {
        return executor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }
    
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return executor.submit(task);
    }
    
    @Override
    public Future<?> submit(Runnable task) {
        return executor.submit(task);
    }
    
    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return executor.submit(task, result);
    }
    
    @Override
    public boolean isTerminated() {
        return false;
    }
    
    @Override
    public boolean isShutdown() {
        return false;
    }
    
    
    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
        return ImmutableList.of();
    }
}
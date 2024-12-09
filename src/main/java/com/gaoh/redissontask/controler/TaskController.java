package com.gaoh.redissontask.controler;

import com.gaoh.redissontask.task.ExecuteQueueTaskService;
import com.gaoh.redissontask.task.QueueTask;
import com.gaoh.redissontask.task.QueueTaskService;
import com.gaoh.redissontask.vo.RestResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

/**
 * @author KangJunJie
 */
@RestController
public class TaskController {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private QueueTaskService queueTaskService;


    @GetMapping("task")
    public RestResult<String> getTask() {
        stringRedisTemplate.opsForHash().put("redisson-task", "1", System.currentTimeMillis() + "");
        return RestResult.ok("redisson-task");
    }


    @GetMapping("addTask")
    public RestResult<String> addTask() {
        queueTaskService.addTask(new QueueTask(ExecuteQueueTaskService.TASK_01, "666", LocalDateTime.now().plusMinutes(1)));
        return RestResult.ok();
    }


    @GetMapping("addDelayedTask")
    public RestResult<String> addDelayedTask(@RequestParam long delayInSec) {
        // 创建一个队列任务
        QueueTask queueTask = new QueueTask(ExecuteQueueTaskService.TASK_01, "任务内容", LocalDateTime.now().plusSeconds(delayInSec));

        // 调用 addTaskWithDelay 方法，将任务添加到延迟队列
        queueTaskService.addTaskWithDelay(queueTask, delayInSec);

        return RestResult.ok("任务已添加，延迟时间：" + delayInSec + "秒");
    }

    @GetMapping("removeTask")
    public RestResult<String> removeTask() {
        queueTaskService.removeTask(new QueueTask(ExecuteQueueTaskService.TASK_01, "666", LocalDateTime.now().plusMinutes(1)));
        return RestResult.ok();
    }

}

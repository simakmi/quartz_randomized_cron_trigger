package ru.test;

import java.text.ParseException;
import java.util.Date;
import java.util.TimeZone;

import org.quartz.CronTrigger;
import org.quartz.ScheduleBuilder;
import org.quartz.spi.MutableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomizedCronScheduleBuilder extends ScheduleBuilder<CronTrigger> {
    private static final Logger log = LoggerFactory.getLogger(RandomizedCronScheduleBuilder.class);

    private final String cronExpression;
    private TimeZone timeZone;
    private Date startTime;

    private RandomizedCronScheduleBuilder(String cronExpression) {
        if (cronExpression == null) {
            throw new IllegalArgumentException("Cron expression is required, must not be null");
        }
        this.cronExpression = cronExpression;
    }

    @Override
    protected MutableTrigger build() {
        RandomizedCronTrigger trigger = new RandomizedCronTrigger();

        try {
            trigger.setCronExpression(cronExpression);
        } catch (ParseException exc) {
            throw new RuntimeException("CronExpression '" + cronExpression + "' is invalid.", exc);
        }

        if (timeZone != null) {
            trigger.setTimeZone(timeZone);
        }

        if (startTime != null) {
            trigger.setStartTime(startTime);
        }

        trigger.setMisfireInstruction(CronTrigger.MISFIRE_INSTRUCTION_SMART_POLICY);

        log.debug("Created: {}", trigger);

        return trigger;
    }

    public static RandomizedCronScheduleBuilder cronSchedule(String expression) {
        log.debug("Randomized cron expression: {}", expression);
        return new RandomizedCronScheduleBuilder(expression);
    }

    public RandomizedCronScheduleBuilder inTimeZone(TimeZone timezone) {
        this.timeZone = timezone;
        return this;
    }

    public RandomizedCronScheduleBuilder startTime(Date startTime) {
        this.startTime = startTime;
        return this;
    }
}

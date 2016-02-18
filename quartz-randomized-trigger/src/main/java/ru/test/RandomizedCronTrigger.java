package ru.test;

import java.text.ParseException;
import java.util.Date;
import java.util.TimeZone;

import org.quartz.Calendar;
import org.quartz.CronExpression;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.ScheduleBuilder;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.triggers.CoreTrigger;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.test.util.CronExpressionUtil;

public class RandomizedCronTrigger implements OperableTrigger, CronTrigger, CoreTrigger {
    private static final Logger log = LoggerFactory.getLogger(RandomizedCronTrigger.class);
    
    private static final long serialVersionUID = -4742794309319331324L;

    private String cronExpression;
    private CronExpression randomizedCronExpression;
    private CronTriggerImpl cronTrigger;
    
    public RandomizedCronTrigger() {
        cronTrigger = new CronTriggerImpl();
    }
    
    @Override
    public Object clone() {
        RandomizedCronTrigger copy;
        
        try {
            copy = (RandomizedCronTrigger) super.clone();
            copy.cronTrigger = (CronTriggerImpl) cronTrigger.clone();
        } catch (CloneNotSupportedException ex) {
            throw new IncompatibleClassChangeError("Not Cloneable.");
        }
        
        return copy;
    }

    public void setCronExpression(String cronExpression) throws ParseException {
        this.cronExpression = cronExpression;
        try {
            cronExpression = CronExpressionUtil.buildExpression(cronExpression);
            this.randomizedCronExpression = new CronExpression(cronExpression);
        } catch (ParseException exc) {
            throw new IllegalArgumentException("Invalid cron expression '" + cronExpression + "'", exc);
        }
        
        log.debug("Set cron expression: {}", cronExpression);
        cronTrigger.setCronExpression(randomizedCronExpression);
    }
    
    public void setTimeZone(TimeZone timezone) {
        cronTrigger.setTimeZone(timezone);
    }
    
    @Override
    public String getCronExpression() {
        return cronExpression;
    }

    @Override
    public void setNextFireTime(Date nextFireTime) {
        log.debug("Set next fire time: {}", nextFireTime);
        cronTrigger.setNextFireTime(nextFireTime);
    }

    @Override
    public void setPreviousFireTime(Date previousFireTime) {
        log.debug("Set previous fire time: {}", previousFireTime);
        cronTrigger.setPreviousFireTime(previousFireTime);
    }

    @Override
    public boolean hasAdditionalProperties() {
        return false;
    }

    @Override
    public TimeZone getTimeZone() {
        return cronTrigger.getTimeZone();
    }

    @Override
    public String getExpressionSummary() {
        return cronTrigger.getExpressionSummary();
    }

    @Override
    public void triggered(Calendar calendar) {
        Date previousFireTime = cronTrigger.getNextFireTime();
        cronTrigger.setPreviousFireTime(previousFireTime);
        log.debug("Previous fire time: {}", previousFireTime);
        
        Date nextFireTime = getFireTimeAfter(previousFireTime);
        
        long diff = (getFireTimeAfter(nextFireTime).getTime() - nextFireTime.getTime())/2;
        long currentTime = System.currentTimeMillis();
        
        log.debug("Half of period: {}", diff);
        while (nextFireTime != null && nextFireTime.getTime() - currentTime < diff) {
            log.debug("...next fire time: {}");
            nextFireTime = getFireTimeAfter(nextFireTime);
        }

        while (nextFireTime != null && calendar != null
                && !calendar.isTimeIncluded(nextFireTime.getTime())) {
            nextFireTime = getFireTimeAfter(nextFireTime);
        }
        
        log.debug("Triggered next fire time: {}", nextFireTime);
        cronTrigger.setNextFireTime(nextFireTime);
    }

    @Override
    public Date computeFirstFireTime(Calendar calendar) {
        return cronTrigger.computeFirstFireTime(calendar);
    }

    @Override
    public boolean mayFireAgain() {
        return cronTrigger.mayFireAgain();
    }

    @Override
    public Date getStartTime() {
        return cronTrigger.getStartTime();
    }

    @Override
    public void setStartTime(Date startTime) {
        cronTrigger.setStartTime(startTime);
    }

    @Override
    public void setEndTime(Date endTime) {
        cronTrigger.setEndTime(endTime);
    }

    @Override
    public Date getEndTime() {
        return cronTrigger.getEndTime();
    }

    @Override
    public Date getNextFireTime() {
        return cronTrigger.getNextFireTime();
    }

    @Override
    public Date getPreviousFireTime() {
        return cronTrigger.getPreviousFireTime();
    }

    @Override
    public Date getFireTimeAfter(Date afterTime) {
        return cronTrigger.getFireTimeAfter(afterTime);
    }

    @Override
    public Date getFinalFireTime() {
        return cronTrigger.getFinalFireTime();
    }

    @Override
    public void updateAfterMisfire(Calendar cal) {
        cronTrigger.updateAfterMisfire(cal);
    }

    @Override
    public void updateWithNewCalendar(Calendar calendar, long misfireThreshold) {
        cronTrigger.updateWithNewCalendar(calendar, misfireThreshold);
    }

    @Override
    public ScheduleBuilder<CronTrigger> getScheduleBuilder() {
        return cronTrigger.getScheduleBuilder();
    }

    @Override
    public void setKey(TriggerKey key) {
        cronTrigger.setKey(key);
    }

    @Override
    public void setJobKey(JobKey key) {
        cronTrigger.setJobKey(key);
    }

    @Override
    public void setDescription(String description) {
        cronTrigger.setDescription(description);
    }

    @Override
    public void setCalendarName(String calendarName) {
        cronTrigger.setCalendarName(calendarName);
    }

    @Override
    public void setJobDataMap(JobDataMap jobDataMap) {
        cronTrigger.setJobDataMap(jobDataMap);
    }

    @Override
    public void setPriority(int priority) {
        cronTrigger.setPriority(priority);
    }

    @Override
    public void setMisfireInstruction(int misfireInstruction) {
        cronTrigger.setMisfireInstruction(misfireInstruction);
    }

    @Override
    public TriggerKey getKey() {
        return cronTrigger.getKey();
    }

    @Override
    public JobKey getJobKey() {
        return cronTrigger.getJobKey();
    }

    @Override
    public String getDescription() {
        return cronTrigger.getDescription();
    }

    @Override
    public String getCalendarName() {
        return cronTrigger.getCalendarName();
    }

    @Override
    public JobDataMap getJobDataMap() {
        return cronTrigger.getJobDataMap();
    }

    @Override
    public int getPriority() {
        return cronTrigger.getPriority();
    }

    @Override
    public int getMisfireInstruction() {
        return cronTrigger.getMisfireInstruction();
    }

    @Override
    public TriggerBuilder<CronTrigger> getTriggerBuilder() {
        return cronTrigger.getTriggerBuilder();
    }

    @Override
    public int compareTo(Trigger other) {
        return cronTrigger.compareTo(other);
    }

    @Override
    public CompletedExecutionInstruction executionComplete(JobExecutionContext context, JobExecutionException result) {
        return cronTrigger.executionComplete(context, result);
    }

    @Override
    public void validate() throws SchedulerException {
        cronTrigger.validate();
    }

    @Override
    public void setFireInstanceId(String id) {
        cronTrigger.setFireInstanceId(id);
    }

    @Override
    public String getFireInstanceId() {
        return cronTrigger.getFireInstanceId();
    }
    
    @Override
    public String toString() {
        return "Trigger '" + cronTrigger.getFullName() + "':  triggerClass: '"
                + getClass().getName() + " calendar: '" + cronTrigger.getCalendarName() 
                + "' misfireInstruction: " + cronTrigger.getMisfireInstruction() 
                + " nextFireTime: " + cronTrigger.getNextFireTime();
    }
}

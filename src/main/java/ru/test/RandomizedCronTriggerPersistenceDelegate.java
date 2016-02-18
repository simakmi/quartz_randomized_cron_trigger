package ru.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.TimeZone;

import org.quartz.JobDetail;
import org.quartz.TriggerKey;
import org.quartz.impl.jdbcjobstore.CronTriggerPersistenceDelegate;
import org.quartz.impl.jdbcjobstore.Util;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomizedCronTriggerPersistenceDelegate extends CronTriggerPersistenceDelegate {
    private static final String TTYPE_R_CRON = "R_CRON";
    
    private static final String UPDATE_JOB_DESCRIPTION = "UPDATE JOB_EXECUTION SET NEXT_TIME = ? WHERE JOB_ID = ?";
    
    private static final Logger log = LoggerFactory.getLogger(RandomizedCronTriggerPersistenceDelegate.class);
    
    @Override
    public String getHandledTriggerTypeDiscriminator() {
        return TTYPE_R_CRON;
    }
    
    @Override
    public boolean canHandleTriggerType(OperableTrigger trigger) {
        return ((trigger instanceof RandomizedCronTrigger) && !((RandomizedCronTrigger)trigger).hasAdditionalProperties());
    }
    
    public int deleteExtendedTriggerProperties(Connection conn, TriggerKey triggerKey) throws SQLException {
        int result = super.deleteExtendedTriggerProperties(conn, triggerKey);
        String name = triggerKey.getName();
        
        if (!name.startsWith("trigger-now")) {
            try {
                int idx = name.lastIndexOf("-");
                String id = name.substring(idx+1);
                updateJobExecutionNextTime(conn, Long.parseLong(id), null);
            } catch (Exception exc) {
                log.warn("Could not update job execution.", exc);
            }
        }
        
        return result;
    }
    
    public int insertExtendedTriggerProperties(Connection conn, OperableTrigger trigger, String state, JobDetail jobDetail) throws SQLException, IOException {
        int result = super.insertExtendedTriggerProperties(conn, trigger, state, jobDetail);
        
        if (trigger.getJobDataMap() != null || trigger.getJobDataMap().containsKey("jobId")) {
            updateJobExecutionNextTime(conn, trigger.getJobDataMap().getLong("jobId"), trigger.getNextFireTime());
        }
        
        return result;
    }
    
    @Override
    public TriggerPropertyBundle loadExtendedTriggerProperties(Connection conn, TriggerKey triggerKey) throws SQLException {

        PreparedStatement ps = null;
        ResultSet rs = null;
        
        try {
            ps = conn.prepareStatement(Util.rtp(SELECT_CRON_TRIGGER, tablePrefix, schedNameLiteral));
            ps.setString(1, triggerKey.getName());
            ps.setString(2, triggerKey.getGroup());
            rs = ps.executeQuery();

            if (rs.next()) {
                String cronExpr = rs.getString(COL_CRON_EXPRESSION);
                String timeZoneId = rs.getString(COL_TIME_ZONE_ID);
                
                RandomizedCronScheduleBuilder cb = RandomizedCronScheduleBuilder.cronSchedule(cronExpr);
              
                if (timeZoneId != null) {
                    cb.inTimeZone(TimeZone.getTimeZone(timeZoneId));
                }
                
                return new TriggerPropertyBundle(cb, null, null);
            }
            
            throw new IllegalStateException("No record found for selection of Trigger with key: '" + triggerKey + "' and statement: " + Util.rtp(SELECT_CRON_TRIGGER, tablePrefix, schedNameLiteral));
        } finally {
            Util.closeResultSet(rs);
            Util.closeStatement(ps);
        }
    }

    @Override
    public int updateExtendedTriggerProperties(Connection conn, OperableTrigger trigger, String state, JobDetail jobDetail) throws SQLException, IOException {
        int result = super.updateExtendedTriggerProperties(conn, trigger, state, jobDetail);
        
        if (trigger.getJobDataMap() != null || trigger.getJobDataMap().containsKey("jobId")) {
            updateJobExecutionNextTime(conn, trigger.getJobDataMap().getLong("jobId"), trigger.getNextFireTime());
        }

        return result;
    }
    
    private void updateJobExecutionNextTime(Connection conn, long jobId, Date nextTime) throws SQLException {
        PreparedStatement ps = null;

        try {
            ps = conn.prepareStatement(UPDATE_JOB_DESCRIPTION);
            
            if (nextTime == null) {
                ps.setNull(1, Types.TIMESTAMP);
            } else {
                ps.setTimestamp(1, new Timestamp(nextTime.getTime()));
            }
            
            ps.setLong(2, jobId);
            
            ps.executeUpdate();
        } finally {
            Util.closeStatement(ps);
        }
    }
}

package ru.test.util;

import static org.quartz.CronScheduleBuilder.cronSchedule;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.triggers.AbstractTrigger;

public final class CronExpressionUtil {

    private static final String LITERAL_R = "R";

    private static final int SECOND = 0;
    private static final int MINUTE = 1;
    private static final int HOUR = 2;
    private static final int DAY_OF_MONTH = 3;
    private static final int MONTH = 4;
    private static final int DAY_OF_WEEK = 5;
    private static final int YEAR = 6;

    private CronExpressionUtil() {
    }
    
    public static String buildExpression(String expression) throws ParseException {
        if (StringUtils.isEmpty(expression)) {
            return expression;
        } else if(!expression.contains(LITERAL_R)) {
            String[] mas = expression.split("[ \t]");
            
            if (mas.length > 6) {
                validateYear(mas[6]);
            }
            
            return expression;
        }

        StringTokenizer expressionToken = new StringTokenizer(expression, " \t", false);

        StringBuilder result = new StringBuilder();
        int exprOn = SECOND;
        while (expressionToken.hasMoreTokens() && exprOn <= YEAR) {
            if (result.length() > 0) {
                result.append(" ");
            }

            String expr = expressionToken.nextToken().trim();

            if (expr.contains(LITERAL_R)) {
                if (!expr.equals(LITERAL_R)) {
                    throw new ParseException(
                            "Illegal cron expression format (" + expr + "), support only 'R' without other characters",
                            0);
                }

                switch (exprOn) {
                case SECOND:
                    result.append(RandomUtils.nextInt(0, 60));
                    break;
                case MINUTE:
                    result.append(RandomUtils.nextInt(0, 60));
                    break;
                case HOUR:
                    result.append(RandomUtils.nextInt(0, 24));
                    break;
                case DAY_OF_MONTH:
                    result.append(RandomUtils.nextInt(1, 29));
                    break;
                case MONTH:
                    result.append(RandomUtils.nextInt(1, 13));
                    break;
                case DAY_OF_WEEK:
                    result.append(RandomUtils.nextInt(1, 8));
                    break;
                case YEAR:
                    throw new ParseException("Invalid year value: '" + expr + "'", -1);
                default:
                    break;
                }
            } else {
                if (exprOn == YEAR) {
                    validateYear(expr);
                }
                
                result.append(expr);
            }

            exprOn++;
        }

        return result.toString();
    }
    
    private static void validateYear(String value) throws ParseException {
        if (!StringUtils.isEmpty(value) && !value.matches("(,|-|\\*|\\/)")) {
            int year = 0;
            try {
                year = Integer.parseInt(value);
            } catch (Exception exc) {};
            int currentYear = Calendar.getInstance().get(Calendar.YEAR);
            if (year < currentYear) {
                throw new ParseException("Incorreсt year value: '" + value + "'", -1);
            }
        }
    }
    
    public static void checkFireTime(String cronExpression) throws SchedulerException {
        AbstractTrigger<?> trigger = (AbstractTrigger<?>) TriggerBuilder.newTrigger()
                .withIdentity("trigger-1", "group")
                .withSchedule(cronSchedule(cronExpression))
                .build();
        
        Date ft = trigger.computeFirstFireTime(null);

        if (ft == null) {
            throw new SchedulerException(
                    "Based on configured schedule, the given trigger will never fire.");
        }
    }

}


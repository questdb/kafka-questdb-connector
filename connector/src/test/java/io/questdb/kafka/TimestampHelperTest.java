package io.questdb.kafka;

import org.junit.jupiter.api.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TimestampHelperTest {

    @Test
    public void testBoundaries_auto() {
        // 4/26/1970, 5:46:40 PM
        Date lowerBound = new Calendar.Builder()
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setDate(1970, 3, 26) // note: month is 0-based
                .setTimeOfDay(17, 46, 40, 1)
                .build().getTime();

        // 11/20/2286, 5:46:40 PM
        Date upperBound = new Calendar.Builder()
                .setTimeZone(TimeZone.getTimeZone("UTC"))
                .setDate(2286, 10, 20) // note: month is 0-based
                .setTimeOfDay(17, 46, 39, 999)
                .build().getTime();

        assertEquals(TimeUnit.MILLISECONDS, TimestampHelper.getTimestampUnits(null, lowerBound.getTime()));
        assertEquals(TimeUnit.MILLISECONDS, TimestampHelper.getTimestampUnits(null, upperBound.getTime()));

        assertEquals(TimeUnit.MICROSECONDS, TimestampHelper.getTimestampUnits(null, lowerBound.getTime() * 1000));
        assertEquals(TimeUnit.MICROSECONDS, TimestampHelper.getTimestampUnits(null, upperBound.getTime() * 1000));

        assertEquals(TimeUnit.NANOSECONDS, TimestampHelper.getTimestampUnits(null, lowerBound.getTime() * 1000000));
        assertEquals(TimeUnit.NANOSECONDS, TimestampHelper.getTimestampUnits(null, Long.MAX_VALUE)); //upper bound in nanos does not fit in long
    }

    @Test
    public void testBoundaries_explicit() {
        assertEquals(TimeUnit.MILLISECONDS, TimestampHelper.getTimestampUnits(TimeUnit.MILLISECONDS, 0));
        assertEquals(TimeUnit.MILLISECONDS, TimestampHelper.getTimestampUnits(TimeUnit.MILLISECONDS, Long.MAX_VALUE));

        assertEquals(TimeUnit.MICROSECONDS, TimestampHelper.getTimestampUnits(TimeUnit.MICROSECONDS, 0));
        assertEquals(TimeUnit.MICROSECONDS, TimestampHelper.getTimestampUnits(TimeUnit.MICROSECONDS, Long.MAX_VALUE));

        assertEquals(TimeUnit.NANOSECONDS, TimestampHelper.getTimestampUnits(TimeUnit.NANOSECONDS, 0));
        assertEquals(TimeUnit.NANOSECONDS, TimestampHelper.getTimestampUnits(TimeUnit.NANOSECONDS, Long.MAX_VALUE));
    }

}
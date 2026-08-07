package org.avniproject.etl.util;

import org.springframework.stereotype.Service;

import java.util.List;

import static java.lang.String.format;

@Service
public class ReportUtil {
    public String getDateDynamicWhere(String startDate, String endDate, String columnName) {
        if (startDate != null) {
            return format("and %s::date between '%s'::date and '%s'::date", columnName, startDate, endDate);
        }
        return "";
    }

    public String getDateSeries(String startDate, String endDate) {
        if (startDate != null) {
            return format("from generate_series('%s'::date - interval '3 months', '%s'::date, '7d'::interval) day", startDate, endDate);
        }
        return "from generate_series(current_date at time zone 'UTC'+'5:30'- interval '3 months' , current_date at time zone 'UTC'+'5:30' , '7d'::interval) day";
    }

    public String getDynamicUserWhere(List<Long> userIds, String columnName) {
        if (!userIds.isEmpty()) {
            return format("and %s in (%s)", columnName, StringUtil.joinLongToList(userIds));
        }
        return "";
    }

}

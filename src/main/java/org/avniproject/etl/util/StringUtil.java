package org.avniproject.etl.util;

import java.util.List;
import java.util.stream.Collectors;

public class StringUtil {

    public static String joinLongToList(List<Long> lists) {
        return lists.isEmpty() ? "" : lists.stream().map(String::valueOf)
                .collect(Collectors.joining(","));
    }

}

package org.avniproject.etl.dto;

import org.avniproject.etl.repository.sql.Page;

import java.math.BigInteger;

public class MediaSearchResponseDTO<T> {
    public int page;
    public T data;
    public BigInteger count;

    public MediaSearchResponseDTO(Page page, T data, BigInteger
                                   count){
        this.page = page.page();
        this.data = data;
        this.count = count;
    }
}

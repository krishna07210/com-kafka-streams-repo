package com.kafka.streams.sorting;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.model.ClicksByNewsType;

import java.io.IOException;
import java.util.TreeSet;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "top3Sorted"
})
public class Top3NewsTypes {

    private ObjectMapper objectMapper = new ObjectMapper();
    public final TreeSet<ClicksByNewsType> top3Sorted = new TreeSet<>((o1, o2) -> {
        final int result = o2.getClicks().compareTo(o1.getClicks());
        if (result > 0)
            return result;
        else
            return o1.getClicks().compareTo(o2.getClicks());
    }
    );

    public void add(ClicksByNewsType newValue) {
        top3Sorted.add(newValue);
        if (top3Sorted.size() > 3) {
            top3Sorted.remove(top3Sorted.last());
        }
    }

    public void remove(ClicksByNewsType oldValue) {
        top3Sorted.remove(oldValue);
    }

    @JsonProperty("top3Sorted")
    public void setTop3Sorted(String top3String) throws IOException {
        ClicksByNewsType[] top3 = objectMapper.readValue(top3String, ClicksByNewsType[].class);
        for (ClicksByNewsType i : top3) {
            add(i);
        }
    }

    @JsonProperty("top3Sorted")
    public String getTop3Sorted() throws JsonProcessingException {
        return objectMapper.writeValueAsString(top3Sorted);
    }
}

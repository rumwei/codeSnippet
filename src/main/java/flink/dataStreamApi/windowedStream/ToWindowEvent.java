package flink.dataStreamApi.windowedStream;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class ToWindowEvent {
    private int country; //1-美国，2-英国，3.德国
    private long trade;
}

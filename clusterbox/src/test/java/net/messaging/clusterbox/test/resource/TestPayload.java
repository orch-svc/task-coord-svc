package net.messaging.clusterbox.test.resource;

import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TestPayload {
    private String total;
    static AtomicInteger counter = new AtomicInteger(0);

    @JsonCreator
    public TestPayload(@JsonProperty("total") String total) {
        super();
        this.total = total;
    }

    public String getTotal() {
        return total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return "TestPayload [total=" + total + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((total == null) ? 0 : total.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TestPayload other = (TestPayload) obj;
        if (total == null) {
            if (other.total != null)
                return false;
        } else if (!total.equals(other.total))
            return false;
        return true;
    }

    public static TestPayload getSamplePaylod() {
        return new TestPayload(String.valueOf(counter.incrementAndGet()));
    }

}

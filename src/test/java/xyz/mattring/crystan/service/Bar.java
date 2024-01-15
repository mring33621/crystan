package xyz.mattring.crystan.service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class Bar extends Foo {
    final Integer val3;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Bar(@JsonProperty("val1") String val1, @JsonProperty("val2") Double val2, @JsonProperty("val3") Integer val3) {
        super(val1, val2);
        this.val3 = val3;
    }

    /**
     * Copy constructor.
     * val3 is calculated as (val1 + val2).hashCode().
     *
     * @param foo
     */
    public Bar(Foo foo) {
        super(foo.getVal1(), foo.getVal2());
        this.val3 = (getVal1() + getVal2()).hashCode();
    }

    public Integer getVal3() {
        return val3;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        Bar bar = (Bar) o;

        return Objects.equals(val3, bar.val3);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (val3 != null ? val3.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Bar{" +
                "val1='" + val1 + '\'' +
                ", val2=" + val2 +
                ", val3='" + val3 + '\'' +
                '}';
    }
}

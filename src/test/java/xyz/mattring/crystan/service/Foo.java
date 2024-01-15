package xyz.mattring.crystan.service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class Foo {
    final String val1;
    final Double val2;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Foo(@JsonProperty("val1") String val1, @JsonProperty("val2") Double val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    public String getVal1() {
        return val1;
    }

    public Double getVal2() {
        return val2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Foo foo = (Foo) o;

        if (!Objects.equals(val1, foo.val1)) return false;
        return Objects.equals(val2, foo.val2);
    }

    @Override
    public int hashCode() {
        int result = val1 != null ? val1.hashCode() : 0;
        result = 31 * result + (val2 != null ? val2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Foo{" +
                "val1='" + val1 + '\'' +
                ", val2=" + val2 +
                '}';
    }
}

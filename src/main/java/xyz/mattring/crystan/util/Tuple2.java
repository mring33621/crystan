package xyz.mattring.crystan.util;

public class Tuple2<T, U> {

        private final T t;
        private final U u;

        public Tuple2(T t, U u) {
            this.t = t;
            this.u = u;
        }

        public T _1() {
            return t;
        }

        public U _2() {
            return u;
        }
}

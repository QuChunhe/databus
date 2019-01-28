package databus.util;

/**
 * Created by Qu Chunhe on 2019-01-23.
 */
public class Tuple2<A, B> {

    public Tuple2(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public final A first;
    public final B second;
}

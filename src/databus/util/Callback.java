package databus.util;

/**
 * Created by Qu Chunhe on 2018-05-28.
 */
public interface Callback<V> {

    void onFailure(Throwable t);

    void onSuccess(V v);
}

package org.mozilla.grouper.base;

/**
 * Supposed to make code a *bit* safer against nulls (until I finally learn Scala).
 * Use it when a final field cannot be set on construction (RAII).
 * Then either just use val (it'll crash in case of null), or pass a Maybe with custom
 * no/yes methods to act on null values.
 */
public abstract class Lazy<T> {
    protected T val_ = null;
    public abstract T make();

    /** crashes if make fails to make something */
    public synchronized final T get() {
        if (val_ == null) val_ = make();
        Assert.nonNull(val_);
        return val_;
    }

    /** act on the value */
    public final void act(Can<T> can) {
        if (val_ == null) val_ = make();
        if (val_ != null) can.yes(val_);
        can.no();
    }

    /** pass by default */
    public static class Can<T> {
        void yes(T val) { /* your code */ }
        void no() { }
    }

    /** fail by default */
    public static class Must<T> extends Can<T> {
        @Override
        void yes(T val) { /* your code */ }
        @Override
        void no() { Assert.unreachable(); }
    }

}

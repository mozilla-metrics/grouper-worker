package org.mozilla.grouper.model;

public interface Ref<T extends Model> {
    Class<T> model();
}

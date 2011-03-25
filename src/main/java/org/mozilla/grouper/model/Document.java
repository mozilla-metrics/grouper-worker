package org.mozilla.grouper.model;

public class Document {

    public Document(DocumentRef ref, String text) {
        ref_ = ref;
        text_ = text;
    }

    public DocumentRef ref() { return ref_; }
    public String text() { return text_; }

    private final DocumentRef ref_;
    private final String text_;
}

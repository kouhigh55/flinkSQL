package org.example.highjoin.entities;

import lombok.Data;

import java.util.HashMap;

@Data
public class Message {
    public HashMap<String, Object> attr;

    public Operation operation;

    public Object keyValue;

    // merge two message
    public HashMap<String, Object> merge(Message child) {
        HashMap<String, Object> mergedAttr = new HashMap<>();
        mergedAttr.putAll(this.attr);
        mergedAttr.putAll(child.attr);
        return mergedAttr;
    }

    // set new key from attr
    public void setKeyValue(String keyName) {
        this.keyValue = this.attr.get(keyName);
    }


}

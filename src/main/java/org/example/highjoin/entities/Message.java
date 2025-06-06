package org.example.highjoin.entities;

import lombok.Data;

import java.util.HashMap;

@Data
public class Message {
    public HashMap<String, Object> attr;

    public Operation operation;

    public Relation targetRelation;

    public Object keyValue;

    public Message(HashMap<String, Object> attr, Operation operation, Relation targetRelation, Object keyValue) {
        this.attr = attr;
        this.operation = operation;
        this.targetRelation = targetRelation;
        this.keyValue = keyValue;
    }

    public Message() {
    }

    // merge two message
    public HashMap<String, Object> merge(Message child) {
        HashMap<String, Object> mergedAttr = new HashMap<>();
        mergedAttr.putAll(this.attr);
        mergedAttr.putAll(child.attr);
        return mergedAttr;
    }

    // set new key from attr
    public boolean setKeyValue(String keyName) {
        if(this.attr.containsKey(keyName)){
            this.keyValue = this.attr.get(keyName);
            return true;
        }else{
            return false;
        }

    }


    public Object getKey() {
        return this.keyValue;
    }

    //clone
    public Message clone(Operation operation, Relation targetRelation, Object keyValue) {
        Message message = new Message();
        message.attr = new HashMap<>(this.attr);
        message.operation = operation;
        message.targetRelation = targetRelation;
        message.keyValue = keyValue;
        return message;
    }
}

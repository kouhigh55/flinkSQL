package org.example.highjoin.entities;

/**
 * INSERT,
 * DELETE,
 * SETALIVE,
 * SETDEAD,
 * ADD,
 * SUBTRACT
 */
public enum Operation {
    INSERT("INSERT"),
    DELETE("DELETE"),
    SETALIVE("SETALIVE"),
    SETDEAD("SETDEAD"),
    ADD("ADD"),
    SUBTRACT("SUBTRACT");


    public final String name;

    Operation(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static Operation fromString(String name) {
        for (Operation op : Operation.values()) {
            if (op.name.equalsIgnoreCase(name)) {
                return op;
            }
        }
        throw new IllegalArgumentException("No enum constant " + Operation.class.getCanonicalName() + "." + name);
    }
}

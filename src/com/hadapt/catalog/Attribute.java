package com.hadapt.catalog;

import java.util.ArrayList;

public class Attribute {
    public final String _name;
    public final String _type;

    public Attribute(String name, String type) {
        _name = name;
        _type = type;
    }

    public boolean equals(Object other) {
        if (other instanceof Attribute) {
            if (((Attribute) other)._name.equalsIgnoreCase(this._name) &&
                ((Attribute) other)._type.equalsIgnoreCase(this._type)) {
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return (_name + _type).toLowerCase().hashCode();
    }
}

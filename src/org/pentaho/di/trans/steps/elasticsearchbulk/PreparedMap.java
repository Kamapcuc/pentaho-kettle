package org.pentaho.di.trans.steps.elasticsearchbulk;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleStepException;

import java.util.HashMap;
import java.util.Map;

public class PreparedMap {

    private Map<String, Map.Entry<String, Object>> fields = new HashMap<String, Map.Entry<String, Object>>();
    private final CloneableMap preparedMap = new CloneableMap();

    private void saveAddress(String address, String fieldName, Map<String, Object> map) {
        map.put(fieldName, null);
        for (Map.Entry<String, Object> entry : map.entrySet())
            if (entry.getKey().equals(fieldName))
                fields.put(address, entry);
    }

    public PreparedMap(Map<String, String> mapping) throws KettleStepException {
        for (String name : mapping.keySet()) {
            if (StringUtils.isNotBlank(name)) {
                if (name.contains(".")) {
                    String[] parts = name.split("\\.");
                    CloneableMap current = preparedMap;
                    for (int j = 0; j < parts.length; j++) {
                        String part = parts[j];
                        if (j < (parts.length - 1)) {
                            Object next = current.get(part);
                            if (next == null) {
                                next = new CloneableMap();
                                current.put(part, next);
                            }
                            if (next instanceof CloneableMap) {
                                current = (CloneableMap) next;
                            } else
                                throw new KettleStepException("Field and object can't have the same name");
                        } else
                            saveAddress(name, part, current);
                    }
                } else
                    saveAddress(name, name, preparedMap);
            }
        }
    }

    public void putValue(String fieldName, Object value) {
        fields.get(fieldName).setValue(value);
    }

    public Map<String, Object> getMap() {
        return preparedMap.deepCopy();
    }

    private final class CloneableMap extends HashMap<String, Object> {
        public CloneableMap deepCopy() {
            CloneableMap tmp = new CloneableMap();
            for (Map.Entry<String, Object> entry : this.entrySet()) {
                if (entry.getValue() instanceof CloneableMap)
                    tmp.put(entry.getKey(), ((CloneableMap) entry.getValue()).deepCopy());
                else
                    tmp.put(entry.getKey(), entry.getValue());
            }
            return tmp;
        }
    }
}

package org.trippi.impl.base;

import java.util.Map;

public class AliasManager {

    private Map<String, String> m_aliasMap;

    public AliasManager(Map<String, String> m) { 
        m_aliasMap = m;
    }

    public synchronized Map<String, String> getAliasMap() {
        return m_aliasMap;
    }

    public synchronized void setAliasMap(Map<String, String> m) {
        m_aliasMap = m;
    }

}

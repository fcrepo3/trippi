package org.trippi.impl.base;

import java.util.Map;

public class AliasManager {

    private Map m_aliasMap;

    public AliasManager(Map m) { 
        m_aliasMap = m;
    }

    public synchronized Map getAliasMap() {
        return m_aliasMap;
    }

    public synchronized void setAliasMap(Map m) {
        m_aliasMap = m;
    }

}

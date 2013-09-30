package org.trippi.impl.base;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.trippi.Alias;

public class DefaultAliasManager implements org.trippi.AliasManager {

    private Map<String, Alias> m_aliasMap;

    public DefaultAliasManager(Map<String, String> m) { 
        m_aliasMap = stringsToAliases(m, m_aliasMap);
    }
    
    public DefaultAliasManager() {
        
    }

    /* (non-Javadoc)
     * @see org.trippi.impl.base.AliasManagerI#getAliasMap()
     */
    @Override
    @Deprecated
    public synchronized Map<String, String> getAliasMap() {
        if (m_aliasMap == null) {
            m_aliasMap = new HashMap<String, Alias>(0);
        }
        HashMap<String, String> result =
                new HashMap<String, String>(m_aliasMap.size());
        for (Entry<String, Alias> entry: m_aliasMap.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getExpansion());
        }
        return result;
    }
    
    /* (non-Javadoc)
     * @see org.trippi.impl.base.AliasManagerI#addAlias(java.lang.String, java.lang.String)
     */
    @Override
    public synchronized void addAlias(String alias, String fullForm) {
        if (m_aliasMap == null) {
            m_aliasMap = new HashMap<String, Alias>();
        }
        m_aliasMap.put(alias, new Alias(alias, fullForm));
    }

    /* (non-Javadoc)
     * @see org.trippi.impl.base.AliasManagerI#setAliasMap(java.util.Map)
     */
    @Override
    @Deprecated
    public synchronized void setAliasMap(Map<String, String> m) {
        m_aliasMap = stringsToAliases(m, m_aliasMap);
    }
    
    /* (non-Javadoc)
     * @see org.trippi.impl.base.AliasManagerI#setAliases(java.util.Map)
     */
    @Override
    public synchronized void setAliases(Map<String, Alias> aliasMap) {
        m_aliasMap = aliasMap;
    }
    
    /* (non-Javadoc)
     * @see org.trippi.impl.base.AliasManagerI#getAliases()
     */
    @Override
    public synchronized Map<String, Alias> getAliases() {
        if (m_aliasMap == null){
            m_aliasMap = new HashMap<String, Alias>(0);
        }
        return m_aliasMap;
    }
    
    private static Map<String, Alias> stringsToAliases(
            Map<String, String> strings, Map<String, Alias> aliases) {
        if (aliases == null){
            aliases = new HashMap<String, Alias>(strings.size());
        }
        for (Entry<String,String> entry: strings.entrySet()) {
            String key = entry.getKey();
            Alias alias = aliases.get(key);
            if (alias == null || !alias.getExpansion().equals(entry.getValue())){
                aliases.put(key, new Alias(key, entry.getValue()));
            }
        }
        return aliases;
    }

}

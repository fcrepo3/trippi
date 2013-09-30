
package org.trippi;

import java.util.Map;


public interface AliasManager {

    @Deprecated
    public abstract Map<String, String> getAliasMap();

    public abstract void addAlias(String alias, String fullForm);

    @Deprecated
    public abstract void setAliasMap(Map<String, String> m);

    public abstract void setAliases(Map<String, Alias> aliasMap);

    public abstract Map<String, Alias> getAliases();

}
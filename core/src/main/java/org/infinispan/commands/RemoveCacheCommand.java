package org.infinispan.commands;

import org.infinispan.Cache;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Command to stop a cache and remove all its contents from both
 * memory and any backing store.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class RemoveCacheCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 18;

   private EmbeddedCacheManager cacheManager;
   private GlobalComponentRegistry registry;
   private PersistenceManager persistenceManager;

   private RemoveCacheCommand() {
      super(null); // For command id uniqueness test
   }

   public RemoveCacheCommand(String cacheName, EmbeddedCacheManager cacheManager,
         GlobalComponentRegistry registry, PersistenceManager persistenceManager) {
      super(cacheName);
      this.cacheManager = cacheManager;
      this.registry = registry;
      this.persistenceManager = persistenceManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // To avoid reliance of a thread local flag, get a reference for the
      // cache store to be able to clear it after cache has stopped.
      Cache<Object, Object> cache = cacheManager.getCache(cacheName);
      CacheJmxRegistration jmx = cache.getAdvancedCache().getComponentRegistry().getComponent(CacheJmxRegistration.class);
      cache.stop();

      // After stopping the cache, clear it
      if (persistenceManager != null)
         persistenceManager.clearAllStores(false);

      // And see if we need to remove it from JMX
      if (jmx != null) {
         jmx.unregisterCacheMBean();
      }

      registry.removeCache(cacheName);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return Util.EMPTY_OBJECT_ARRAY;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      // No parameters
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}

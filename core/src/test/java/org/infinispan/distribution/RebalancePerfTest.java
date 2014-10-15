package org.infinispan.distribution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.impl.TopologyAwareConsistentHashFactory;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTopologyAwareAddress;
import org.infinispan.test.AbstractInfinispanTest;
import org.jgroups.util.TopologyUUID;
import org.jgroups.util.UUID;
import org.testng.annotations.Test;

/**
 * Tests ConsistentHashFactory performance.
 *
 * @author Takayoshi Kimura
 * @since 7.0
 */
@Test(testName = "distribution.RebalancePerfTest", groups = "manual")
public class RebalancePerfTest extends AbstractInfinispanTest {

   private static int NUM_INITIAL_NODES = 1;
   private static int NUM_MAX_NODES = 500;
   private static int NUM_OWNERS = 2;
   private static int NUM_SEGMENTS = NUM_MAX_NODES * 10;
   private static boolean ENABLE_CAPACITY_FACTOR = true;

   public void testTopologyAwareConsistentHash() {
      ConsistentHashFactory factory = new TopologyAwareConsistentHashFactory();
      Map<Address, Float> capacityFactors = ENABLE_CAPACITY_FACTOR ? new HashMap<Address, Float>() : null;

      List<Address> addresses = new ArrayList<Address>();
      for (int i = 0; i < NUM_INITIAL_NODES; i++) {
         Address address = new JGroupsTopologyAwareAddress(
            TopologyUUID.randomUUID("logical-name" + i, "site1", "rackA", "machine" + i));
         addresses.add(address);
         if (capacityFactors != null) {
            capacityFactors.put(address, 1.0f);
         }
      }

      ConsistentHash pendingCH = factory.create(new MurmurHash3(), NUM_OWNERS, NUM_SEGMENTS, addresses, capacityFactors);
      ConsistentHash balancedCH = factory.rebalance(pendingCH);

      List<Address> newMembers = new ArrayList<Address>();
      for (int i = NUM_INITIAL_NODES; i < NUM_MAX_NODES; i++) {
         String rack = i % 2 == 0 ? "rackA" : "rackB";
         Address address = new JGroupsTopologyAwareAddress(
            TopologyUUID.randomUUID("logical-name" + i, "site1", "rackA", "machine" + i));
         newMembers.add(address);
         if (capacityFactors != null) {
            capacityFactors.put(address, 1.0f);
         }
      }

      long start = System.currentTimeMillis();
      pendingCH = factory.updateMembers(balancedCH, newMembers, capacityFactors);
      long middle = System.currentTimeMillis();
      balancedCH = factory.rebalance(pendingCH);
      long end = System.currentTimeMillis();

      System.out.printf("half=%d\n", middle - start);
      System.out.printf("time=%d\n", end - start);
   }

}

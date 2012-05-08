/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.extended.loadbalance.strategies.hotspot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.DefaultLoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;

import com.google.common.collect.HashBiMap;

public abstract class HotSpotLoadBalancer extends DefaultLoadBalancer {
	private ClusterStatus status;
	private float slop;
	protected Configuration config;
	protected MasterServices services;

	private static final Log LOG = LogFactory.getLog(HotSpotLoadBalancer.class);
	private double hotspotLoadPercentThreshold;
	private double hotspotLoadNumberRegionsThreshold;

	public double getHotspotLoadPercentThreshold() {
		return hotspotLoadPercentThreshold;
	}

	public double getHotspotLoadNumberRegionsThreshold() {
		return hotspotLoadNumberRegionsThreshold;
	}

	@Override
	public void setConf(Configuration conf) {
		this.slop = conf.getFloat("hbase.regions.slop", (float) 0.2);
		if (slop < 0)
			slop = 0;
		else if (slop > 1)
			slop = 1;
		hotspotLoadPercentThreshold = (double)conf.getFloat("hbase.extended.loadbalance.strategies.hotspot.percentthresh", (float)0.9);
		hotspotLoadNumberRegionsThreshold = (double)conf.getFloat("hbase.extended.loadbalance.strategies.hotspot.regionthresh", (float)0.5);
		this.config = conf;
	}
	
	@Override
	public List<RegionPlan> balanceCluster(
			Map<ServerName, List<HRegionInfo>> clusterState) {
		LOG.info("#################Came in the new Balancer Code");
		long startTime = System.currentTimeMillis();
		LOG.info("#################startTime = " + startTime);
		int numServers = clusterState.size();
		LOG.info("#################numServers = " + numServers);
		if (numServers == 0) {
			LOG.debug("numServers=0 so skipping load balancing");
			LOG.info("#################numServers=0 so skipping load balancing");
			return null;
		}

		/**
		 * <pre>
		 * We atleast need two priority queues 
		 * a) It would contain HotSpot regions with their load as the moving criteria (max priority queue)
		 * b) Non hot spot region with their loads (min priority queue)
		 * 
		 * Further we need to iterate over these queues and decrease the load so we 
		 * need a data structure to build these queues 
		 * and lastly we need to return the Region plan.
		 * </pre>
		 */

		NavigableMap<HotSpotServerAndLoad, List<HotSpotRegionLoad>> serversByLoad = new TreeMap<HotSpotServerAndLoad, List<HotSpotRegionLoad>>();
		PriorityQueue<HotSpotServerAndLoad> hotspotRegionServers = new PriorityQueue<HotSpotServerAndLoad>(
				numServers, HotSpotServerAndLoad.DESC_LOAD);
		PriorityQueue<HotSpotServerAndLoad> nonHotspotRegionServers = new PriorityQueue<HotSpotServerAndLoad>(
				numServers, HotSpotServerAndLoad.ASC_LOAD);

		HashBiMap<HRegionInfo, HotSpotRegionLoad> allRegionsBiMap = HashBiMap
				.create();

		double normalisedTotalLoadOfAllRegions = initRegionLoadMapsBasedOnInput(
				clusterState, serversByLoad, allRegionsBiMap);
		LOG.info("#################normalisedTotalLoadOfAllRegions=" + normalisedTotalLoadOfAllRegions);
		// Check if we even need to do any load balancing
		double average = normalisedTotalLoadOfAllRegions / numServers; // for
																		// logging
		// HBASE-3681 check sloppiness first
		if (!loadBalancingNeeded(numServers, serversByLoad,
				normalisedTotalLoadOfAllRegions, average)) {
			// we do not need load balancing
			return null;
		}

		// but there could be a condition where nonHotspotRegionServers are also
		// loaded
		// so we will not add them

		double minLoad = normalisedTotalLoadOfAllRegions / numServers;
		double maxLoad = normalisedTotalLoadOfAllRegions % numServers == 0 ? minLoad
				: minLoad + 1;
		// as we now have to balance stuff, init PQ's
		
		LOG.info(String.format("#################minLoad =%s,maxLoad= %s",
				minLoad, maxLoad));
		for (Map.Entry<HotSpotServerAndLoad, List<HotSpotRegionLoad>> item : serversByLoad
				.entrySet()) {
			HotSpotServerAndLoad serverLoad = item.getKey();
			if (serverLoad.isHotSpot()) {

				hotspotRegionServers.add(serverLoad);
			} else {
				if (serverLoad.getLoad() < maxLoad) {
					nonHotspotRegionServers.add(serverLoad);
				}
			}
		}

		// Using to check balance result.
		StringBuilder strBalanceParam = new StringBuilder();
		strBalanceParam.append("Balance parameter: numRegions=")
				.append(normalisedTotalLoadOfAllRegions)
				.append(", numServers=").append(numServers).append(", max=")
				.append(maxLoad).append(", min=").append(minLoad);
		LOG.debug(strBalanceParam.toString());

		// Balance the cluster

		List<RegionPlan> regionsToReturn = new ArrayList<RegionPlan>();
		LOG.info(String.format("#################hotspotRegionServers.size() =%s,nonHotspotRegionServers.size()= %s",
				hotspotRegionServers.size(), nonHotspotRegionServers.size()));
		while (hotspotRegionServers.size() > 0
				&& nonHotspotRegionServers.size() > 0) {
			HotSpotServerAndLoad serverToBalance = hotspotRegionServers.poll();
			LOG.info(String.format("#################serverToBalance =%s",
					serverToBalance.getServerName().getServerName()));
			// get least loaded not hotspot regions of this server
			List<HotSpotRegionLoad> regionList = serversByLoad
					.get(serverToBalance);
			// assume it to be sorted asc.
			if (regionList.size() > 0) {
				HotSpotRegionLoad regionToMove = regionList.remove(0);
				HRegionInfo regionMoveInfo = allRegionsBiMap.inverse().get(
						regionToMove);
				LOG.info(String
						.format("#################regionMoveInfo =%s, metaRegion=%s, isRegionHotspot=%s ",
								regionMoveInfo.getEncodedName(),
								regionMoveInfo.isMetaRegion(),
								regionToMove.isRegionHotspot()));
				if (!regionMoveInfo.isMetaRegion()
						&& !regionToMove.isRegionHotspot()) {
					// move out.
					HotSpotServerAndLoad destinationServer = nonHotspotRegionServers
							.poll();
					
					RegionPlan rpl = new RegionPlan(allRegionsBiMap.inverse()
							.get(regionToMove),
							serverToBalance.getServerName(),
							destinationServer.getServerName());
					regionsToReturn.add(rpl);
					serverToBalance.modifyLoad(regionToMove.getLoad());
					destinationServer.modifyLoad(-1 * regionToMove.getLoad());
					// reenter them to list. if they satisfy conditions
					if (serverToBalance.getLoad() > minLoad) {
						hotspotRegionServers.offer(serverToBalance);
					}
					if (destinationServer.getLoad() < maxLoad) {
						nonHotspotRegionServers.offer(destinationServer);
					}
				}
			}
		}
		LOG.info("Total Time taken to balance = "
				+ (System.currentTimeMillis() - startTime));
		LOG.info(String.format("#################regionsToReturn=%s ",
				regionsToReturn));
		
		return regionsToReturn;
	}

	private boolean loadBalancingNeeded(
			int numServers,
			NavigableMap<HotSpotServerAndLoad, List<HotSpotRegionLoad>> serversByLoad,
			double normalisedTotalLoadOfAllRegions, double average) {
		double floor = Math.floor(average * (1 - slop));
		double ceiling = Math.ceil(average * (1 + slop));
		if (serversByLoad.lastKey().getLoad() <= ceiling
				&& serversByLoad.firstKey().getLoad() >= floor) {
			// as it is sorted ascending we know that the lastKey has the most
			// load.
			// Skipped because no server outside (min,max) range
			LOG.info("##########Skipping load balancing because balanced cluster; "
					+ "servers=" + numServers + " " + "regions="
					+ normalisedTotalLoadOfAllRegions + " average=" + average
					+ " " + "mostloaded=" + serversByLoad.lastKey().getLoad()
					+ " leastloaded=" + serversByLoad.firstKey().getLoad());
			return false;
		}
		return true;
	}

	private double initRegionLoadMapsBasedOnInput(
			Map<ServerName, List<HRegionInfo>> clusterState,
			NavigableMap<HotSpotServerAndLoad, List<HotSpotRegionLoad>> serversByLoad,
			HashBiMap<HRegionInfo, HotSpotRegionLoad> allRegionsBiMap) {
		double normalisedTotalLoadOfAllRegionServers = 0.0;
		HServerLoad regionServerLoad = null;
		for (Map.Entry<ServerName, List<HRegionInfo>> regionserver : clusterState
				.entrySet()) {
			regionServerLoad = status.getLoad(regionserver.getKey());
			Map<byte[], RegionLoad> regionalLoadMapforServer = regionServerLoad
					.getRegionsLoad();
			Map<byte[], HRegionInfo> inputNameRegionInfoMap = createRegionNameRegionInfoMapFromList(regionserver
					.getValue());
			double loadAllRegionsOneRegionServer = addRegionsToCompleteMap(
					inputNameRegionInfoMap, regionserver.getKey(), serversByLoad,
					regionalLoadMapforServer, allRegionsBiMap);

			normalisedTotalLoadOfAllRegionServers += loadAllRegionsOneRegionServer;
			LOG.info("#################server.getKey().getServerName=" + regionserver.getKey().getServerName());
			// serversByLoad.put(new ModifiedServerAndLoad(server.getKey(),
			// loadOnThisRegion), regions);
		}
		return normalisedTotalLoadOfAllRegionServers;
	}

	private Map<byte[], HRegionInfo> createRegionNameRegionInfoMapFromList(
			List<HRegionInfo> listRegionInfo) {
		Map<byte[], HRegionInfo> map = new HashMap<byte[], HRegionInfo>();
		for (HRegionInfo hri : listRegionInfo) {
			map.put(hri.getEncodedNameAsBytes(), hri);
		}
		return map;
	}

	private double addRegionsToCompleteMap(
			Map<byte[], HRegionInfo> inputNameRegionInfoMap,
			ServerName serverName,
			NavigableMap<HotSpotServerAndLoad, List<HotSpotRegionLoad>> serversByLoad,
			Map<byte[], RegionLoad> inputRegionalLoadMapforServer,
			HashBiMap<HRegionInfo, HotSpotRegionLoad> allRegionsBiMap) {
		double loadAccumulator = 0.0;
		List<HotSpotRegionLoad> modRegionLoadList = new ArrayList<HotSpotRegionLoad>();
		boolean isHotspot = false;
		HRegionInfo regionInfo = null;
		TreeMap<HotSpotRegionLoad, HRegionInfo> regionLoadMap = new TreeMap<HotSpotRegionLoad, HRegionInfo>(
				HotSpotRegionLoad.DESC_LOAD);
		for (Map.Entry<byte[], RegionLoad> loadItem : inputRegionalLoadMapforServer
				.entrySet()) {
			// loadItem.getKey == regionCompleteEncodedName
			regionInfo = inputNameRegionInfoMap.get(loadItem.getKey());
			HotSpotRegionLoad readHotSpotRegionLoad = getHotSpotRegionLoadInstance(loadItem);
			regionLoadMap.put(readHotSpotRegionLoad, regionInfo);
			loadAccumulator += readHotSpotRegionLoad.getLoad();
			modRegionLoadList.add(readHotSpotRegionLoad);
			allRegionsBiMap.put(regionInfo, readHotSpotRegionLoad);
			LOG.info("#################readHotSpotRegionLoad=" + readHotSpotRegionLoad.getRegionName());

		}
		// iterate over regionLoadMap and find if the top x% have y% load
		isHotspot = isHotSpot(regionLoadMap, loadAccumulator);
		HotSpotServerAndLoad msl = new HotSpotServerAndLoad(serverName,
				loadAccumulator, isHotspot);
		Collections.sort(modRegionLoadList, HotSpotRegionLoad.DESC_LOAD);
		serversByLoad.put(msl, modRegionLoadList);

		return loadAccumulator;

	}

	/**
	 * For each class that is derived from this load balancer should override
	 * this method and return a subclass of HotSpotRegionLoad which will need to
	 * define the process for calculating the criteria for load, which is futher
	 * used to make hotspot determination.
	 */
	protected abstract HotSpotRegionLoad getHotSpotRegionLoadInstance(
			Map.Entry<byte[], RegionLoad> loadItem);

	private boolean isHotSpot(
			TreeMap<HotSpotRegionLoad, HRegionInfo> regionLoadMap,
			double totalLoad) {
		// iterate in order on HotSpotRegionLoad
		boolean isHotSpot = false;
		double loadTillNow = 0.0;
		HotSpotRegionLoad load = null;
		List<HotSpotRegionLoad> listToMarkHotSpot = new ArrayList<HotSpotRegionLoad>();
		int counter = 0;
		double hotspotLoad = totalLoad * hotspotLoadPercentThreshold;
		int hotspotNumber = (int) (regionLoadMap.size() * hotspotLoadNumberRegionsThreshold);
		for (Map.Entry<HotSpotRegionLoad, HRegionInfo> regionLoadItem : regionLoadMap
				.entrySet()) {
			load = regionLoadItem.getKey();
			loadTillNow += load.getLoad();
			counter++;
			if (loadTillNow >= hotspotLoad) {
				// reached hotspot
				if (counter < hotspotNumber) {
					// hotspot reached
					listToMarkHotSpot.add(load);
					isHotSpot = true;
					break;
				} else {
					break;
				}
			} else {
				// potentially hotspot
				listToMarkHotSpot.add(load);
			}
		}
		if (isHotSpot) {
			// need to mark the list as true.
			for (HotSpotRegionLoad item : listToMarkHotSpot) {
				item.setRegionHotspot(true);
			}
		}

		return isHotSpot;
	}

}

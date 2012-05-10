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
	protected long divideFactor;

	public double getHotspotLoadPercentThreshold() {
		return hotspotLoadPercentThreshold;
	}

	public void setClusterStatus(ClusterStatus st) {
		this.status = st;
	}

	public double getHotspotLoadNumberRegionsThreshold() {
		return hotspotLoadNumberRegionsThreshold;
	}

	@Override
	public void setConf(Configuration conf) {
		this.config = conf;
	}

	private void initParameters() {
		this.slop = this.config.getFloat("hbase.regions.slop", (float) 0.2);
		if (slop < 0)
			slop = 0;
		else if (slop > 1)
			slop = 1;
		hotspotLoadPercentThreshold = (double) this.config.getFloat(
				"hbase.extended.loadbalance.strategies.hotspot.percentthresh",
				(float) 0.9);
		hotspotLoadNumberRegionsThreshold = (double) this.config.getFloat(
				"hbase.extended.loadbalance.strategies.hotspot.regionthresh",
				(float) 0.5);
		divideFactor = this.config
				.getLong("hbase.loadbalancer.dividefactor", 1);
	}

	@Override
	public List<RegionPlan> balanceCluster(
			Map<ServerName, List<HRegionInfo>> clusterState) {
		initParameters();
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

		LOG.debug("#################Came in the new Balancer Code and the cluster status is = "
				+ this.status);
		long startTime = System.currentTimeMillis();
		int numServers = clusterState.size();
		if (numServers == 0) {
			LOG.info("numServers=0 so skipping load balancing");
			return null;

		}

		NavigableMap<HotSpotServerAndLoad, List<HotSpotRegionLoad>> regionServerAndServerLoadMap = new TreeMap<HotSpotServerAndLoad, List<HotSpotRegionLoad>>();
		PriorityQueue<HotSpotServerAndLoad> hotspotRegionServers = new PriorityQueue<HotSpotServerAndLoad>(
				numServers, HotSpotServerAndLoad.DESC_LOAD);
		PriorityQueue<HotSpotServerAndLoad> nonHotspotRegionServers = new PriorityQueue<HotSpotServerAndLoad>(
				numServers, HotSpotServerAndLoad.ASC_LOAD);
		HashBiMap<HRegionInfo, HotSpotRegionLoad> allRegionsLoadBiMap = HashBiMap
				.create();
		LOG.debug("#################clusterState=" + clusterState);
		double normalisedTotalLoadOfAllRegions = initRegionLoadMapsBasedOnInput(
				clusterState, regionServerAndServerLoadMap, allRegionsLoadBiMap);
		LOG.debug("#################normalisedTotalLoadOfAllRegions="
				+ normalisedTotalLoadOfAllRegions);
		// Check if we even need to do any load balancing
		double average = normalisedTotalLoadOfAllRegions / numServers; // for
		// logging
		// HBASE-3681 check sloppiness first
		LOG.debug("######################## final regionServerAndServerLoadMap == "
				+ regionServerAndServerLoadMap);
		if (!loadBalancingNeeded(numServers, regionServerAndServerLoadMap,
				normalisedTotalLoadOfAllRegions, average)) {
			// we do not need load balancing
			return null;
		}
		double minLoad = normalisedTotalLoadOfAllRegions / numServers;
		double maxLoad = normalisedTotalLoadOfAllRegions % numServers == 0 ? minLoad
				: minLoad + 1;
		// as we now have to balance stuff, init PQ's
		LOG.debug(String.format("#################minLoad =%s,maxLoad= %s",
				minLoad, maxLoad));
		for (Map.Entry<HotSpotServerAndLoad, List<HotSpotRegionLoad>> item : regionServerAndServerLoadMap
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
		List<RegionPlan> regionsToReturn = new ArrayList<RegionPlan>();
		
		while (hotspotRegionServers.size() > 0
				&& nonHotspotRegionServers.size() > 0) {
			HotSpotServerAndLoad serverToBalance = hotspotRegionServers.poll();
			LOG.debug(String.format("#################serverToBalance =%s",
					serverToBalance.getServerName().getServerName()));
			// get least loaded not hotspot regions of this server
			List<HotSpotRegionLoad> regionList = regionServerAndServerLoadMap
					.get(serverToBalance);
			// assume it to be sorted asc.
			if (regionList.size() > 0) {
				HotSpotRegionLoad regionToMove = regionList.remove(0);
				HRegionInfo regionMoveInfo = allRegionsLoadBiMap.inverse().get(
						regionToMove);
				
				/*
				 * regionMoveInfo can be null in case the load map returns us
				 * the root and meta regions along with the movable regions But
				 * as the clusterState which is passed to us does not contain
				 * these regions we can have a situation where
				 * regionServerAndServerLoadMap contains some regions which are
				 * not present in the allRegionsLoadBiMap
				 */
				if (regionMoveInfo != null && !regionMoveInfo.isMetaRegion()
						&& !regionMoveInfo.isRootRegion()
						&& !regionMoveInfo.isMetaTable()
						&& regionToMove.isRegionHotspot()) {
					LOG.debug(String
							.format("#################Came to move the region regionMoveInfo=%s;; regionToMove=%s ",
									regionMoveInfo, regionToMove));
					// move out.
					HotSpotServerAndLoad destinationServer = nonHotspotRegionServers
							.poll();

					RegionPlan rpl = new RegionPlan(allRegionsLoadBiMap
							.inverse().get(regionToMove),
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
		if (serversByLoad.size() > 0) {
			if (serversByLoad.lastKey().getLoad() <= ceiling
					&& serversByLoad.firstKey().getLoad() >= floor) {
				// as it is sorted ascending we know that the lastKey has the
				// most
				// load.
				// Skipped because no server outside (min,max) range
				LOG.info("##########Skipping load balancing because balanced cluster; "
						+ "servers="
						+ numServers
						+ " "
						+ "regions="
						+ normalisedTotalLoadOfAllRegions
						+ " average="
						+ average
						+ " "
						+ "mostloaded="
						+ serversByLoad.lastKey().getLoad()
						+ " leastloaded="
						+ serversByLoad.firstKey().getLoad());
				return false;
			} else {
				// only case where load balancing is required
				return true;
			}
		}
		return false;
	}

	/**
	 * Iterate and initialize serversByLoad and allRegionsBiMap on the basis of
	 * clusterState
	 * 
	 * 
	 */
	private double initRegionLoadMapsBasedOnInput(
			Map<ServerName, List<HRegionInfo>> clusterState,
			NavigableMap<HotSpotServerAndLoad, List<HotSpotRegionLoad>> regionServerAndServerLoadMap,
			HashBiMap<HRegionInfo, HotSpotRegionLoad> allRegionsLoadBiMap) {
		double normalisedTotalLoadOfAllRegionServers = 0.0;
		
		for (Map.Entry<ServerName, List<HRegionInfo>> regionServerRegionEntry : clusterState
				.entrySet()) {
			LOG.debug("#################initRegionLoadMapsBasedOnInput: regionServerRegionEntry.getKey()="
					+ regionServerRegionEntry.getKey());

			Map<String, HRegionInfo> regionNameRegionInfoMap = createRegionNameRegionInfoMapFromList(regionServerRegionEntry
					.getValue());

			HServerLoad regionServerLoad = status
					.getLoad(regionServerRegionEntry.getKey());
			LOG.debug("#################initRegionLoadMapsBasedOnInput:regionServerLoad="
					+ regionServerLoad);
			Map<byte[], RegionLoad> regionalLoadMapforServerOld = regionServerLoad
					.getRegionsLoad();
			Map<String, RegionLoad> regionalLoadMapforServer = new HashMap<String, HServerLoad.RegionLoad>();
			for (Map.Entry<byte[], RegionLoad> entry : regionalLoadMapforServerOld
					.entrySet()) {
				//TODO: Remove this log
				LOG.debug(String
						.format("#################initRegionLoadMapsBasedOnInput: entry.getKey()=%s , entry.getValue() = %s, "
								+ "entry.getValue().getName()= %s, entry.getValue().getNameAsString()= %s",
								entry.getKey(), entry.getValue(), entry
										.getValue().getName(), entry.getValue()
										.getNameAsString()));
				HRegionInfo regionInfoFromClusterState = regionNameRegionInfoMap
						.get(entry.getValue().getNameAsString());
				if (regionInfoFromClusterState != null) {
					if (regionInfoFromClusterState.isMetaRegion()
							|| regionInfoFromClusterState.isMetaTable()
							|| regionInfoFromClusterState.isRootRegion()) {
						// do not enter as we will not move these regions
						LOG.debug(String
								.format("#################initRegionLoadMapsBasedOnInput: regionInfoFromClusterState = %s is "
										+ "not entered in the  regionalLoadMapforServer as we will not load balance this region due to meta nature",
										regionInfoFromClusterState));
					} else {
						regionalLoadMapforServer.put(entry.getValue()
								.getNameAsString(), entry.getValue());
					}
				} else {
					LOG.debug(String
							.format("#################initRegionLoadMapsBasedOnInput: entry = %s from  regionalLoadMapforServerOld"
									+ " does not exists in regionNameRegionInfoMap",
									entry));
				}

			}
			LOG.debug("#################initRegionLoadMapsBasedOnInput: regionalLoadMapforServer="
					+ regionalLoadMapforServer);

			if (regionalLoadMapforServer.size() <= 1) {
				LOG.info("#################initRegionLoadMapsBasedOnInput: as regionalLoadMapforServer<=1."
						+ " We do not need to balance it");
			} else {
				
				double loadAllRegionsOneRegionServer = addRegionsToCompleteMap(
						regionNameRegionInfoMap,
						regionServerRegionEntry.getKey(),
						regionServerAndServerLoadMap, regionalLoadMapforServer,
						allRegionsLoadBiMap);
				LOG.debug("#################initRegionLoadMapsBasedOnInput: loadAllRegionsOneRegionServer="
						+ loadAllRegionsOneRegionServer);
				LOG.debug("#################initRegionLoadMapsBasedOnInput: allRegionsBiMap="
						+ allRegionsLoadBiMap);
				normalisedTotalLoadOfAllRegionServers += loadAllRegionsOneRegionServer;
			}

		}
		return normalisedTotalLoadOfAllRegionServers;
	}

	private Map<String, HRegionInfo> createRegionNameRegionInfoMapFromList(
			List<HRegionInfo> listRegionInfo) {
		Map<String, HRegionInfo> map = new HashMap<String, HRegionInfo>();
		for (HRegionInfo hri : listRegionInfo) {
			map.put(hri.getRegionNameAsString(), hri);
		}
		return map;
	}

	private double addRegionsToCompleteMap(
			Map<String, HRegionInfo> regionNameRegionInfoMap,
			ServerName serverName,
			NavigableMap<HotSpotServerAndLoad, List<HotSpotRegionLoad>> serversByLoad,
			Map<String, RegionLoad> regionalLoadMapforServer,
			HashBiMap<HRegionInfo, HotSpotRegionLoad> allRegionsLoadBiMap) {
		double loadAccumulator = 0.0;
		List<HotSpotRegionLoad> modRegionLoadList = new ArrayList<HotSpotRegionLoad>();
		boolean isHotspot = false;
		HRegionInfo regionInfo = null;
		TreeMap<HotSpotRegionLoad, HRegionInfo> regionLoadMap = new TreeMap<HotSpotRegionLoad, HRegionInfo>(
				HotSpotRegionLoad.DESC_LOAD);
		
		for (Map.Entry<String, RegionLoad> loadItem : regionalLoadMapforServer
				.entrySet()) {

			regionInfo = regionNameRegionInfoMap.get(loadItem.getKey());
			if (regionInfo == null) {
				String message = "######################## as regionInfo is null from regionNameRegionInfoMap for the region name ="
						+ loadItem.getKey()
						+ " determined from  regionNameRegionInfoMap. The rest of the balancing is useless. "
						+ "We need to return to the assignment manager.";
				LOG.warn(message);
			} else {
				HotSpotRegionLoad readHotSpotRegionLoad = getHotSpotRegionLoadInstance(
						loadItem, this.divideFactor);
				LOG.debug("######################## loadItem = " + loadItem
						+ "\n readHotSpotRegionLoad= " + readHotSpotRegionLoad);
				regionLoadMap.put(readHotSpotRegionLoad, regionInfo);
				loadAccumulator += readHotSpotRegionLoad.getLoad();
				LOG.debug("######################## current loadAccumulator="
						+ loadAccumulator);
				modRegionLoadList.add(readHotSpotRegionLoad);
				allRegionsLoadBiMap.put(regionInfo, readHotSpotRegionLoad);
			}

		}
		// iterate over regionLoadMap and find if the top x% have y% load
		isHotspot = isHotSpot(regionLoadMap, modRegionLoadList, loadAccumulator);
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
			Map.Entry<String, RegionLoad> loadItem, long pDivideFactor);

	private boolean isHotSpot(
			TreeMap<HotSpotRegionLoad, HRegionInfo> regionLoadMap,
			List<HotSpotRegionLoad> exisitingRegionLoadList, double totalLoad) {
		// iterate in order on HotSpotRegionLoad
		boolean isHotSpot = false;
		double loadTillNow = 0.0;
		HotSpotRegionLoad load = null;
		List<HotSpotRegionLoad> listToMarkHotSpot = new ArrayList<HotSpotRegionLoad>();
		int counter = 0;
		double hotspotLoad = totalLoad * hotspotLoadPercentThreshold;
		LOG.debug("#################isHotSpot: hotspotLoad=" + hotspotLoad
				+ " totalLoad= " + totalLoad
				+ " hotspotLoadPercentThreshold = "
				+ hotspotLoadPercentThreshold);
		int hotspotNumber = (int) Math.ceil(regionLoadMap.size()
				* hotspotLoadNumberRegionsThreshold);

		LOG.debug("#################isHotSpot: hotspotNumber=" + hotspotNumber
				+ " regionLoadMap.size()= " + regionLoadMap.size()
				+ " hotspotLoadNumberRegionsThreshold = "
				+ hotspotLoadNumberRegionsThreshold);
		for (Map.Entry<HotSpotRegionLoad, HRegionInfo> regionLoadItem : regionLoadMap
				.entrySet()) {
			load = regionLoadItem.getKey();
			loadTillNow += load.getLoad();
			counter++;
			LOG.debug(String
					.format("#################isHotSpot: load = %s;loadTillNow=%s; hotspotLoad=%s; counter=%s ",
							load, loadTillNow, hotspotLoad, counter));
			if (loadTillNow >= hotspotLoad) {
				LOG.debug(String
						.format("#################isHotSpot: counter = %s;hotspotNumber=%s; ",
								counter, hotspotNumber));

				if (counter <= hotspotNumber) {
					// hotspot reached
					listToMarkHotSpot.add(load);
					isHotSpot = true;
					break;
				} else {
					break;
				}
			} else {
				LOG.debug(String
						.format("#################isHotSpot: Adding load = %s into potential hotspot list",
								load));
				// potentially hotspot
				listToMarkHotSpot.add(load);
			}
		}
		LOG.debug(String
				.format("#################isHotSpot: isHotSpot =%s ; ;;listToMarkHotSpot = %s;exisitingRegionLoadList=%s ",
						isHotSpot, listToMarkHotSpot, exisitingRegionLoadList));

		if (isHotSpot) {
			// need to mark the list as true.
			for (HotSpotRegionLoad item : listToMarkHotSpot) {
				int itemIndexIfExists = exisitingRegionLoadList.indexOf(item);
				LOG.debug(String
						.format("#################isHotSpot: Item =%s is in the exitisting list at index =%s ",
								item, itemIndexIfExists));
				if (itemIndexIfExists >= 0) {
					exisitingRegionLoadList.get(itemIndexIfExists)
							.setRegionHotspot(true);
				}
			}
		}

		LOG.debug("#################isHotSpot: listToMarkHotSpot List="
				+ listToMarkHotSpot);
		return isHotSpot;
	}

}

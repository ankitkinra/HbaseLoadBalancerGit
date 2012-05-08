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

import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.extended.loadbalance.strategies.serverregionload.ModifiedRegionLoad;

/**
 * Generic class without calculateLoad implementation which is left for the
 * concrete class.
 * 
 */
public abstract class HotSpotRegionLoad extends ModifiedRegionLoad {

	private boolean regionHotspot = false;

	public HotSpotRegionLoad(RegionLoad rl) {
		super(rl);
	}

	public HotSpotRegionLoad(HotSpotRegionLoad rl) {
		super(rl.getBaseRegionLoad());
	}

	public boolean isRegionHotspot() {
		return regionHotspot;
	}

	public void setRegionHotspot(boolean regionHotspot) {
		this.regionHotspot = regionHotspot;
	}
	
	@Override
	protected void calculateLoad() {
		throw new UnsupportedOperationException("Pleaes subclass this class and then use this method");
		
	}
}

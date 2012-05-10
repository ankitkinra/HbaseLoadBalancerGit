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
package org.apache.hadoop.hbase.extended.loadbalance.strategies.hotspot.readhotspot;

import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.extended.loadbalance.strategies.hotspot.HotSpotRegionLoad;

public class ReadHotSpotRegionLoad extends HotSpotRegionLoad {

	public ReadHotSpotRegionLoad(RegionLoad rl, long pDivideFactor) {
		super(rl, pDivideFactor);

	}

	/**
	 * this is the main method which needs to be changed when we have a new
	 * strategy for calculating the load.
	 */
	public void calculateLoad() {
		// right now just comparing the read requests
		this.setLoad(this.getReadRequests() / this.divideFactor);
	}

}

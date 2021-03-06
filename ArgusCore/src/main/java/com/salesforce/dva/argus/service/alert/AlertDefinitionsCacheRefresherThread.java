/*
 * Copyright (c) 2018, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * 3. Neither the name of Salesforce.com nor the names of its contributors may
 * be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.salesforce.dva.argus.service.alert;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.dva.argus.entity.Alert;
import com.salesforce.dva.argus.service.AlertService;
import com.salesforce.dva.argus.service.MonitorService.Counter;

public class AlertDefinitionsCacheRefresherThread extends Thread{

	private final Logger _logger = LoggerFactory.getLogger(AlertDefinitionsCacheRefresherThread.class);

	// keeping the refresh interval at 1 minute, as this corresponds to the minimum alert execution interval based on cron expression
	private static final Long REFRESH_INTERVAL_MILLIS = 60*1000L;

	private static final Long LOOKBACK_PERIOD_FOR_REFRESH_MILLIS = 5*REFRESH_INTERVAL_MILLIS;

	private AlertDefinitionsCache alertDefinitionsCache = null;

	private AlertService alertService;

	public AlertDefinitionsCacheRefresherThread(AlertDefinitionsCache cache, AlertService alertService) {
		this.alertDefinitionsCache = cache;
		this.alertService = alertService;
	}

	public void run() {
		long lastExecutionTime = 0L;
		while (!isInterrupted()) {
			long executionTime = 0L, currentExecutionTime = 0L;
			try {
				_logger.info("Starting alert definitions cache refresh");
				long startTime = System.currentTimeMillis();
				if(!alertDefinitionsCache.isAlertsCacheInitialized()) {
					List<Alert> enabledAlerts = alertService.findAlertsByStatus(true);
					lastExecutionTime = System.currentTimeMillis();
					Map<BigInteger, Alert> enabledAlertsMap = enabledAlerts.stream().collect(Collectors.toMap(alert -> alert.getId(), alert -> alert));
					for(Alert a : enabledAlerts) {
						addEntrytoCronMap(a);
					}
					alertDefinitionsCache.setAlertsMapById(enabledAlertsMap);
					alertDefinitionsCache.setAlertsCacheInitialized(true);
				}else {
					List<Alert> modifiedAlerts = alertService.findAlertsModifiedAfterDate(new Date(startTime - Math.max(executionTime + REFRESH_INTERVAL_MILLIS, LOOKBACK_PERIOD_FOR_REFRESH_MILLIS)));
					currentExecutionTime = System.currentTimeMillis();
					// updating only the modified/deleted alerts in the cache
					long sumTimeToDiscover = 0L;
					long sumTimeToDiscoverNew = 0L;
					int newAlertsCount = 0;
					int updatedAlertsCount = 0;
					if(modifiedAlerts!=null && modifiedAlerts.size()>0) {
						for(Alert a : modifiedAlerts) {
							// calculate the time to discover the update or the creation
							long timeToDiscover = currentExecutionTime - a.getModifiedDate().getTime();
							
							// if the creationTime is after last execution, then we treat this as newly
							// created and record the time to discover of newly created alert.
							// NOTE: due to time difference between DB and this machine there could be 
							//       miss datapoint, I hope that by judging whether updated time is less
							//       then 1 seconds away from the creation time, we can recognized
							//       the potentially missed creation.
							if (lastExecutionTime > 0) {
								if (a.getCreatedDate().getTime() >= lastExecutionTime && a.getCreatedDate().getTime() < currentExecutionTime) {
									timeToDiscover = currentExecutionTime - a.getCreatedDate().getTime();
									sumTimeToDiscoverNew += timeToDiscover;
									newAlertsCount ++;
									_logger.debug("Found new alert {} which was created at {}, lastExecutionTime {}, currentExecutionTime {}, timeToDiscover {}", 
											a.getId().toString(), a.getCreatedDate().getTime(), lastExecutionTime, currentExecutionTime, timeToDiscover);
								}else if (a.getModifiedDate().getTime() >= lastExecutionTime) {
									sumTimeToDiscover += timeToDiscover;
									updatedAlertsCount ++;
								}
							}
							_logger.debug("Processing modified alert - {},{},{},{} after {} milliseconds ", a.getId(),
									a.getName(), a.getCronEntry(), a.getExpression(), timeToDiscover);
							if(alertDefinitionsCache.getAlertsMapById().containsKey(a.getId())) {
								if(a.isDeleted() || !a.isEnabled()) {
									alertDefinitionsCache.getAlertsMapById().remove(a.getId());  
									removeEntryFromCronMap(a.getId());
								}else {
									alertDefinitionsCache.getAlertsMapById().put(a.getId(), a);    
									// removing the previous cron mapping and adding fresh just in case the mapping changed
									removeEntryFromCronMap(a.getId());
									addEntrytoCronMap(a);
								}
							}else if(a.isEnabled() && !a.isDeleted()) {
								alertDefinitionsCache.getAlertsMapById().put(a.getId(), a);
								addEntrytoCronMap(a);
							}
						}
					}
					
					alertService.updateCounter(Counter.ALERTS_UPDATED_COUNT, (double)modifiedAlerts.size());
					_logger.info("Number of modified alerts since last refresh - " + modifiedAlerts.size());
					
					if (updatedAlertsCount > 0) {
						long avgTimeToDiscover = sumTimeToDiscover / updatedAlertsCount;
						alertService.updateCounter(Counter.ALERTS_UPDATE_LATENCY, (double)avgTimeToDiscover);
						_logger.info("Average time to discovery of change - " + avgTimeToDiscover + " milliseconds");
					}

					if (newAlertsCount > 0) {
						_logger.info("Number of created alerts since last refresh - " + newAlertsCount);
						long avgTimeToDiscoverNewAlert = sumTimeToDiscoverNew / newAlertsCount;
						alertService.updateCounter(Counter.ALERTS_NEW_LATENCY, (double)avgTimeToDiscoverNewAlert);
						_logger.info("Average time to discovery of new alert - " + avgTimeToDiscoverNewAlert + " milliseconds");
					}
				}
				
				if (lastExecutionTime > 0) {
					_logger.info("AlertCache was refreshed after {} millisec", currentExecutionTime - lastExecutionTime);
				}

				lastExecutionTime = currentExecutionTime;
				executionTime = System.currentTimeMillis() - startTime;
				_logger.info("Alerts cache refreshed successfully in {} millis. Number of alerts in cache - {}", executionTime, alertDefinitionsCache.getAlertsMapById().keySet().size());
				if(executionTime < REFRESH_INTERVAL_MILLIS) {
					sleep(REFRESH_INTERVAL_MILLIS - executionTime);
				}
			}catch(Exception e) {
				_logger.error("Exception occured when trying to refresh alert definition cache - " + ExceptionUtils.getFullStackTrace(e));
			}
		}
	}

	private void addEntrytoCronMap(Alert a) {
		if(alertDefinitionsCache.getAlertsMapByCronEntry().get(a.getCronEntry())==null) {
			alertDefinitionsCache.getAlertsMapByCronEntry().put(a.getCronEntry(), new ArrayList<BigInteger>());
		}
		alertDefinitionsCache.getAlertsMapByCronEntry().get(a.getCronEntry()).add(a.getId());
	}
	
	private void removeEntryFromCronMap(BigInteger alertId) {
		for(String cronEntry : alertDefinitionsCache.getAlertsMapByCronEntry().keySet()) {
			if(alertDefinitionsCache.getAlertsMapByCronEntry().get(cronEntry).contains(alertId)) {
				alertDefinitionsCache.getAlertsMapByCronEntry().get(cronEntry).remove(alertId);
			}
		}
	}
}

package com.salesforce.dva.argus.service.monitor;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.*;

import com.salesforce.dva.argus.service.MonitorService;
import com.salesforce.dva.argus.service.alert.notifier.GusNotifier;
import javafx.util.Pair;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.dva.argus.entity.Metric;
import com.salesforce.dva.argus.service.MailService;
import com.salesforce.dva.argus.service.MetricService;
import com.salesforce.dva.argus.system.SystemConfiguration;

/*
 * This class runs a thread which periodically checks if there is data lag on Argus side.
 * 
 */
public class DataLagMonitor extends Thread{

	private String _dataLagQueryExpressions;

	private long _dataLagThreshold;

	private String _dataLagNotificationEmailId;

	private String _hostName;

	private Map<String, Boolean> _isDataLaggingbyDCMap = new TreeMap<>();

	private Map<String, String> _expressionPerDC = new TreeMap<>();

	private MetricService _metricService;

	private MonitorService _monitorService;

	private MailService _mailService;

	private static final Long SLEEP_INTERVAL_MILLIS = 60*1000L;

	private final Logger _logger = LoggerFactory.getLogger(DataLagMonitor.class);

	private SystemConfiguration _sysConfig;

    private Set<String> listOfDCForNotificationDataLagPresent = new HashSet<>();
    private Set<String> listOfDCForNotificationDataLagNotPresent = new HashSet<>();
    private boolean isThereAnyChangesInDataLag = false;

	public DataLagMonitor(SystemConfiguration sysConfig, MetricService metricService, MailService mailService, MonitorService monitorService) {
	    _sysConfig = sysConfig;
		_metricService = metricService;
		_mailService = mailService;
		_dataLagQueryExpressions = sysConfig.getValue(SystemConfiguration.Property.DATA_LAG_QUERY_EXPRESSION);
        _dataLagThreshold = Long.valueOf(sysConfig.getValue(com.salesforce.dva.argus.system.SystemConfiguration.Property.DATA_LAG_THRESHOLD));
		_dataLagNotificationEmailId = sysConfig.getValue(com.salesforce.dva.argus.system.SystemConfiguration.Property.DATA_LAG_NOTIFICATION_EMAIL_ADDRESS);
		_hostName = sysConfig.getHostname();
		_monitorService = monitorService;
		initExpressionList(_dataLagQueryExpressions);
        _logger.info("[dataLagMonitor] Data lag monitor initialized");
	}

	private void initExpressionList(String dataLagQueryExpressions) {
	    for(String expressionDCPair: dataLagQueryExpressions.split("&&")) {
	        String [] currentExpressionDC = expressionDCPair.split("\\|\\|");
	        String currentExpression = currentExpressionDC[0];
            for (String currentDC : currentExpressionDC[1].split(",")) {
                _expressionPerDC.put(currentDC, currentExpression.replace("#DC#", currentDC));
                _isDataLaggingbyDCMap.put(currentDC, false);
            }
        }
    }

	@Override
	public void run() {
		_logger.info("[dataLagMonitor] Data lag monitor thread started");
        final ExecutorService pool = Executors.newFixedThreadPool(5);
        final ExecutorCompletionService<Pair<String, List<Metric>>> completionService = new ExecutorCompletionService<>(pool);
        long currTime = System.currentTimeMillis();
		boolean firstTime = true;

        while (!isInterrupted()) {
            try {
                if (!firstTime) {
                    sleep(SLEEP_INTERVAL_MILLIS);
                    isThereAnyChangesInDataLag = false;
                } else {
                    // waiting 5 seconds for everything to initialize
                    sleep(5 * 1000);
                    firstTime = false;
                }

                for (String currentDC : _expressionPerDC.keySet()) {
                    completionService.submit(() -> {
                        List<Metric> metrics = new ArrayList<>();
                        try {
                            metrics = _metricService.getMetrics(_expressionPerDC.get(currentDC), currTime);
                        } catch (Exception e) {
                            metrics.clear();
                            _logger.error("[dataLagMonitor] Metric Service failed to get metric for expression: " + _expressionPerDC.get(currentDC) + " while being queried by DataLagMonitor.");
                        }
                        return new Pair<>(currentDC, metrics);
                    });
                }
                for (int idx = 0; idx < _expressionPerDC.size(); ++idx) {
                    Future<Pair<String, List<Metric>>> future = completionService.take();
                    Pair<String, List<Metric>> result = future.get();
                    String currentDC = result.getKey();
                    List<Metric> metrics = result.getValue();
                    boolean isDataLagging = _isDataLaggingbyDCMap.get(currentDC);
                    if (metrics == null || metrics.isEmpty()) {
                        _logger.info("[dataLagMonitor] Data lag detected as metric list is empty: " + _expressionPerDC.get(idx));
                        if (!isDataLagging) {
                            updatesForDataLag(currentDC, true);
                        }
                        continue;
                    }

                    //assuming only one time series in result
                    Metric currMetric = metrics.get(0);
                    if (currMetric.getDatapoints() == null || currMetric.getDatapoints().size() == 0) {
                        _logger.info("[dataLagMonitor] Data lag detected as data point list is empty: " + _expressionPerDC.get(idx));
                        if (!isDataLagging) {
                            updatesForDataLag(currentDC, true);
                        }
                        continue;
                    } else {
                        long lastDataPointTime = 0L;
                        for (Long dataPointTime : currMetric.getDatapoints().keySet()) {
                            if (dataPointTime > lastDataPointTime) {
                                lastDataPointTime = dataPointTime;
                            }
                        }
                        if ((currTime - lastDataPointTime) > _dataLagThreshold) {
                            _logger.info("[dataLagMonitor] Data lag detected as the last data point recieved is more than the data threshold of " + _dataLagThreshold + " ms: " + _expressionPerDC.get(idx));
                            if (!isDataLagging) {
                                updatesForDataLag(currentDC, true);
                            }
                            continue;
                        }
                    }
                    if (isDataLagging) {
                        updatesForDataLag(currentDC, false);
                        _logger.info("[dataLagMonitor Data lag has been cleared for DC: " + _expressionPerDC.get(idx));
                        sendDataLagNotification(new ArrayList<>(listOfDCForNotificationDataLagNotPresent), false);
                    }
                }
                if (isThereAnyChangesInDataLag){
					sendDataLagNotification(new ArrayList<>(listOfDCForNotificationDataLagPresent), true);
					_logger.info("[dataLagMonitor] Changes in data lag DCs detected");
				} else {
					_logger.info("[dataLagMonitor] No Change in data lag DCs detected");
				}
            } catch (Exception e) {
                _logger.error("[dataLagMonitor] Exception thrown in data lag monitor thread - " + ExceptionUtils.getFullStackTrace(e));
            }
        }
	}

	private void updatesForDataLag(String currentDC, boolean isDataLaggingInDC) {
        this._isDataLaggingbyDCMap.put(currentDC, isDataLaggingInDC);
        if (isDataLaggingInDC) {
            isThereAnyChangesInDataLag = true;
            listOfDCForNotificationDataLagPresent.add(currentDC);
            listOfDCForNotificationDataLagNotPresent.remove(currentDC);
        } else {
            listOfDCForNotificationDataLagNotPresent.add(currentDC);
            listOfDCForNotificationDataLagPresent.remove(currentDC);
        }
    }

	private void sendDataLagNotification(List<String> dcList, boolean isDataLagDCList) {
		Set<String> emailAddresseses = new HashSet<String>();
		emailAddresseses.add(_dataLagNotificationEmailId);
		String subject = "";
		int delta = 0;
		String dcListString = dcList.toString();
		if(isDataLagDCList) {
			subject = "[dataLagMonitor]Alert evaluation on host - "+ _hostName + " has been stopped due to metric data lag in the following datacenters: " + dcListString;
			delta = 1;
		}else {
			subject = "[dataLagMonitor]Alert evaluation on host - "+ _hostName + " has been resumed as the metric data lag has cleared in the following datacenters: " + dcListString;
			delta = -1;
		}

		StringBuilder body = new StringBuilder();
		body.append("<b>Evaluated metric expression:  </b> <br/>");
		for(String currentDC: dcList) {
		    body.append(_expressionPerDC.get(currentDC) + "<br/>");
            Map<String, String> tags = new HashMap<>();
            tags.put("DC", currentDC);
            _monitorService.modifyCounter(MonitorService.Counter.DATALAG_PER_DC_COUNT, delta, tags);
        }
        body.append(MessageFormat.format("<b>Configured data lag threshold:  </b> {0}<br/>", _dataLagThreshold));
		
		_mailService.sendMessage(emailAddresseses, subject, body.toString(), "text/html; charset=utf-8", MailService.Priority.NORMAL);
		_logger.info("Email sending successful.");
//        GusNotifier.postToGus(new HashSet<String>(Arrays.asList(_sysConfig.getValue(SystemConfiguration.Property.ARGUS_GUS_GROUP_ID))), subject, _sysConfig, true);

//        _logger.info("[dataLagMonitor] Posting to GUS and sending Email: \n" + subject + "\nBody: \n" + body);
	}

	public boolean isDataLagging(String currentDC) {
		return _isDataLaggingbyDCMap.get(currentDC);
	}
}

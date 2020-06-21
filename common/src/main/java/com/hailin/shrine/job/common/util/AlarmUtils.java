package com.hailin.shrine.job.common.util;


import com.google.gson.reflect.TypeToken;
import com.hailin.shrine.job.common.exception.ShrineJobException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.Map;

public class AlarmUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(AlarmUtils.class);

	/**
	 * Send alarm request to Alarm API in Console.
	 */
	public static void raiseAlarm(Map<String, Object> alarmInfo, String namespace) throws ShrineJobException {
		int size = SystemEnvProperties.SCHEDULE_CONSOLE_URI_LIST.size();
		for (int i = 0; i < size; i++) {

			String consoleUri = SystemEnvProperties.SCHEDULE_CONSOLE_URI_LIST.get(i);
			String targetUrl = consoleUri + "/rest/v1/" + namespace + "/alarms/raise";

			LOGGER.info( LogEvents.ExecutorEvent.COMMON,
					"raise alarm of domain {} to url {}: {}, retry count: {}", namespace, targetUrl,
					alarmInfo.toString(), i);
			CloseableHttpClient httpClient = null;
			try {
				checkParameters(alarmInfo);
				// prepare
				httpClient = HttpClientBuilder.create().build();
				HttpPost request = new HttpPost(targetUrl);
				final RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000)
						.setSocketTimeout(10000).build();
				request.setConfig(requestConfig);
				StringEntity params = new StringEntity(
						JsonUtils.getGson().toJson(alarmInfo, new TypeToken<Map<String, Object>>() {
						}.getType()));
				request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
				request.setEntity(params);

				// send request
				CloseableHttpResponse httpResponse = httpClient.execute(request);
				// handle response
				HttpUtils.handleResponse(httpResponse);
				return;
			} catch (ShrineJobException se) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, "SaturnJobException throws: {}", se.getMessage(),
						se);
				throw se;
			} catch (ConnectException e) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, "Fail to connect to url:{}, throws: {}", targetUrl,
						e.getMessage(), e);
				if (i == size - 1) {
					throw new ShrineJobException(ShrineJobException.SYSTEM_ERROR, "no available console server", e);
				}
			} catch (Exception e) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, "Other exception throws: {}", e.getMessage(), e);
				throw new ShrineJobException(ShrineJobException.SYSTEM_ERROR, e.getMessage(), e);
			} finally {
				HttpUtils.closeHttpClientQuietly(httpClient);
			}
		}
	}

	private static void checkParameters(Map<String, Object> alarmInfo) throws ShrineJobException {
		if (alarmInfo == null) {
			throw new ShrineJobException(ShrineJobException.ILLEGAL_ARGUMENT, "alarmInfo cannot be null.");
		}

		String level = (String) alarmInfo.get("level");
		if (StringUtils.isBlank(level)) {
			throw new ShrineJobException(ShrineJobException.ILLEGAL_ARGUMENT, "level cannot be blank.");
		}

		String name = (String) alarmInfo.get("name");
		if (StringUtils.isBlank(name)) {
			throw new ShrineJobException(ShrineJobException.ILLEGAL_ARGUMENT, "name cannot be blank.");
		}

		String title = (String) alarmInfo.get("title");
		if (StringUtils.isBlank(title)) {
			throw new ShrineJobException(ShrineJobException.ILLEGAL_ARGUMENT, "title cannot be blank.");
		}

	}

}

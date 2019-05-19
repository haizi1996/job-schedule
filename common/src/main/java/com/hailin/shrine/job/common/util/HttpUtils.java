package com.hailin.shrine.job.common.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.hailin.shrine.job.common.exception.ShrineJobException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HttpUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtils.class);

	public static void handleResponse(CloseableHttpResponse httpResponse) throws IOException, ShrineJobException {
		int status = httpResponse.getStatusLine().getStatusCode();

		if (status >= HttpStatus.SC_OK && status < HttpStatus.SC_MULTIPLE_CHOICES) {
			return;
		}

		if (status >= HttpStatus.SC_BAD_REQUEST && status <= HttpStatus.SC_INTERNAL_SERVER_ERROR) {
			String responseBody = EntityUtils.toString(httpResponse.getEntity());
			if (StringUtils.isNotBlank(responseBody)) {
				JsonElement message = JsonUtils.getJsonParser().parse(responseBody).getAsJsonObject().get("message");
				String errMsg = message == JsonNull.INSTANCE || message == null ? "" : message.getAsString();
				throw new ShrineJobException(ShrineJobException.ILLEGAL_ARGUMENT, errMsg);
			} else {
				throw new ShrineJobException(ShrineJobException.SYSTEM_ERROR, "internal server error");
			}
		} else {
			// if have unexpected status, then throw RuntimeException directly.
			String errMsg = "unexpected status returned from Saturn Server.";
			throw new ShrineJobException(ShrineJobException.SYSTEM_ERROR, errMsg);
		}
	}


	public static void closeHttpClientQuietly(CloseableHttpClient httpClient) {
		if (httpClient != null) {
			try {
				httpClient.close();
			} catch (IOException e) {
				LOGGER.error( LogEvents.ExecutorEvent.COMMON, "Exception during httpclient closed.", e);
			}
		}
	}
}

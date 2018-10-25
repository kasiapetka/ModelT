package com.hybris.datahub.custom;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;

public class CustomSSLValidationService implements InitializingBean
{
	private static final Logger LOGGER = LoggerFactory.getLogger(CustomSSLValidationService.class);
	private boolean httpsEnabled;
	private String datahubServerUrl;
	
	@Override
	public void afterPropertiesSet() throws Exception
	{
		return;
	}
	
	void logWarning()
	{
		LOGGER.warn("\n" +
				"************************************************************************************\n" +
				"* Data Hub is running in HTTP mode.                                                *\n" +
				"*                                                                                  *\n" +
				"* This may be suitable for DEVELOPMENT or in PRODUCTION                            *\n" +
				"* when using a TLS termination proxy.                                              *\n" +
				"* Require HTTPS protocol by setting property datahub.security.https.enabled=true   *\n" +
				"************************************************************************************");
	}

	@Required
	public void setHttpsEnabled(final boolean httpsEnabled)
	{
		this.httpsEnabled = httpsEnabled;
	}

	@Required
	public void setDatahubServerUrl(final String datahubServerUrl)
	{
		this.datahubServerUrl = datahubServerUrl;
	}
}

package de.hybris.y2ysync.task.runner.internal;

import de.hybris.platform.util.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomDataHubRequestCreator extends DataHubRequestCreator
{
	private static final Logger LOG = LoggerFactory.getLogger(CustomDataHubRequestCreator.class);

	@override
	String getY2YSyncWebRoot()
	{
		final String homeUrl = Config.getString("y2ysync.home.url", "http://localhost:9001");
		final String webRoot = "/y2ysync";

		LOG.info("homeUrl = " + homeUrl);
		LOG.info("y2ysyncwebroot = " + homeUrl + webRoot);

		return homeUrl + webRoot;
	}
}
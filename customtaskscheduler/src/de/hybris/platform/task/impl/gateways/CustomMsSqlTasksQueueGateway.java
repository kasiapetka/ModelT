package de.hybris.platform.task.impl.gateways;

import de.hybris.platform.servicelayer.type.TypeService;

import java.text.MessageFormat;

import org.springframework.jdbc.core.JdbcTemplate;


public class CustomMsSqlTasksQueueGateway extends MsSqlTasksQueueGateway
{
	public CustomMsSqlTasksQueueGateway(final JdbcTemplate jdbcTemplate, final TypeService typeService)
	{
		super(jdbcTemplate, typeService);
	}

	@Override
	public String getRangeSQLExpression(final int rangeStart, final int rangeEnd)
	{
		return MessageFormat.format(
				"floor({0,number,#} + (ABS(CAST(CRYPT_GEN_RANDOM(8) AS bigint)) % ({1,number,#}-{0,number,#})))", rangeStart,
				rangeEnd);
	}
}

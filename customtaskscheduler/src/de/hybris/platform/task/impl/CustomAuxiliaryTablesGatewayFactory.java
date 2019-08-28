/*
 * [y] hybris Platform
 *
 * Copyright (c) 2000-2019 SAP SE
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * Hybris ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the
 * terms of the license agreement you entered into with SAP Hybris.
 */
package de.hybris.platform.task.impl;

import de.hybris.platform.servicelayer.type.TypeService;
import de.hybris.platform.task.impl.gateways.CustomMsSqlTasksQueueGateway;
import de.hybris.platform.task.impl.gateways.DefaultTasksQueueGateway;
import de.hybris.platform.task.impl.gateways.HanaTasksQueueGateway;
import de.hybris.platform.task.impl.gateways.MySqlTasksQueueGateway;
import de.hybris.platform.task.impl.gateways.OracleTasksQueueGateway;
import de.hybris.platform.task.impl.gateways.PostgresTasksQueueGateway;
import de.hybris.platform.task.impl.gateways.TasksQueueGateway;
import de.hybris.platform.util.Config;

import org.springframework.beans.factory.annotation.Required;
import org.springframework.jdbc.core.JdbcTemplate;


/**
 *
 */
public class CustomAuxiliaryTablesGatewayFactory extends AuxiliaryTablesGatewayFactory
{
	private JdbcTemplate jdbcTemplate;

	private TypeService typeService;

	@Override
	public TasksQueueGateway getTasksQueueGateway()
	{
		switch (Config.getDatabaseName())
		{
			case HANA:
				return new HanaTasksQueueGateway(jdbcTemplate, typeService);
			case MYSQL:
				return new MySqlTasksQueueGateway(jdbcTemplate, typeService);
			case ORACLE:
				return new OracleTasksQueueGateway(jdbcTemplate, typeService);
			case SQLSERVER:
				return new CustomMsSqlTasksQueueGateway(jdbcTemplate, typeService);
			case POSTGRESQL:
				return new PostgresTasksQueueGateway(jdbcTemplate, typeService);
			default:
				return new DefaultTasksQueueGateway(jdbcTemplate, typeService);
		}
	}

	@Override
	@Required
	public void setJdbcTemplate(final JdbcTemplate jdbcTemplate)
	{
		this.jdbcTemplate = jdbcTemplate;
		super.setJdbcTemplate(jdbcTemplate);
	}

	@Override
	@Required
	public void setTypeService(final TypeService typeService)
	{
		this.typeService = typeService;
		super.setTypeService(typeService);
	}
}

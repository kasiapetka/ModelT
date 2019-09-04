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

import de.hybris.platform.core.Tenant;
import de.hybris.platform.tx.Transaction;
import de.hybris.platform.tx.TransactionBody;

import org.springframework.dao.DataAccessException;


public class CustomTaskDAO extends TaskDAO
{
	private static final int NO_NODE_CLUSTER_ID = -1;
	private final int clusterID;

	public CustomTaskDAO(final Tenant t)
	{
		super(t);
		this.clusterID = super.initializeClusterId();
	}

	@Override
	boolean lock(final Long pk, final long hjmpTS)
	{
		try
		{
			return lockWithTxRequired(pk, hjmpTS);
		}
		catch (final DataAccessException e)
		{
			throw new IllegalStateException("Failed to lock task #" + pk + ".", e);
		}
		catch (final RuntimeException e)
		{
			throw e;
		}
		catch (final Exception e)
		{
			throw new IllegalStateException("Failed to lock task #" + pk + ".", e);
		}
	}

	private boolean lockWithTxRequired(final Long pk, final long hjmpTS) throws Exception
	{
		final Transaction currentTx = Transaction.current();

		if (currentTx.isRunning())
		{
			return lockInternal(pk, hjmpTS);
		}

		final Object txResult = currentTx.execute(new TransactionBody()
		{
			@Override
			public Boolean execute()
			{
				return lockInternal(pk, hjmpTS);
			}
		});

		return Boolean.TRUE.equals(txResult);
	}

	private boolean lockInternal(final Long pk, final long hjmpTS)
	{
		final int updates = this.update(this.getLockQuery(), new Object[]
		{ clusterID, NO_NODE_CLUSTER_ID, pk, hjmpTS });
		if (updates > 0)
		{
			// immediately consume all tasks -> now triggering same id events is creating a new one even record
			update(getConditionConsumeQuery(), new Object[]
			{ Boolean.TRUE, pk });

		}
		return updates > 0;
	}
}

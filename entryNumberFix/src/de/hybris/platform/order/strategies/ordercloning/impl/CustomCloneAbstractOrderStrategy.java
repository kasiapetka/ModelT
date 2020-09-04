/*
 * Copyright (c) 2020 SAP SE or an SAP affiliate company. All rights reserved.
 */
package de.hybris.platform.order.strategies.ordercloning.impl;

import de.hybris.platform.core.Registry;
import de.hybris.platform.core.model.order.AbstractOrderEntryModel;
import de.hybris.platform.core.model.order.AbstractOrderModel;
import de.hybris.platform.order.AbstractOrderEntryTypeService;
import de.hybris.platform.servicelayer.internal.model.impl.ItemModelCloneCreator;
import de.hybris.platform.servicelayer.type.TypeService;

import java.util.List;


/**
 *
 */
public class CustomCloneAbstractOrderStrategy extends DefaultCloneAbstractOrderStrategy
{
	private static final String CLONE_ORDER_STRATEGY_NORMALIZATION = "clone.order.strategy.normalization.legacy.mode";
	private static final String CLONE_ORDER_ENTRY_NUMBER_NORMALIZATION_CADENCE = "clone.order.entry.number.normalization.cadence";

	public CustomCloneAbstractOrderStrategy(final TypeService typeService, final ItemModelCloneCreator itemModelCloneCreator,
			final AbstractOrderEntryTypeService abstractOrderEntryTypeService)
	{
		super(typeService, itemModelCloneCreator, abstractOrderEntryTypeService);
	}

	@Override
	protected void copyCalculatedFlag(final AbstractOrderModel original, final AbstractOrderModel copy)
	{
		copy.setCalculated(original.getCalculated());

		final List<AbstractOrderEntryModel> originalEntries = original.getEntries();
		final List<AbstractOrderEntryModel> copyEntries = copy.getEntries();

		final int copyEntriesSize = copyEntries == null ? 0 : copyEntries.size();

		if (originalEntries.size() != copyEntriesSize)
		{
			throw new IllegalStateException("different entry numbers in original and copied order ( " + originalEntries.size() + "<>"
					+ copyEntries.size() + ")");
		}

		if (isCloneOrderStrategyNormalizationLegacyMode())
		{
			normalizeEntriesNumbers(originalEntries);
		}
		else
		{
			normalizeEntriesNumbers(copyEntries);
		}

		for (int i = 0; i < originalEntries.size(); i++)
		{
			final AbstractOrderEntryModel originalEntry = originalEntries.get(i);
			final AbstractOrderEntryModel copyEntry = copyEntries.get(i);
			copyEntry.setCalculated(originalEntry.getCalculated());
		}
	}

	private static void normalizeEntriesNumbers(final List<AbstractOrderEntryModel> allEntries)
	{
		final int normalizationCadence = getCloneOrderEntryNumberNormalizationCadence();
		for (int i = 0; i < allEntries.size(); i++)
		{
			final AbstractOrderEntryModel oEntry = allEntries.get(i);
			oEntry.setEntryNumber(Integer.valueOf(i) * normalizationCadence);
		}
	}

	private static int getCloneOrderEntryNumberNormalizationCadence()
	{
		int cadence = Registry.getCurrentTenant().getConfig().getInt(CLONE_ORDER_ENTRY_NUMBER_NORMALIZATION_CADENCE, 1);
		if (cadence < 1)
		{
			cadence = 1;
		}
		return cadence;
	}

	private boolean isCloneOrderStrategyNormalizationLegacyMode()
	{
		return Registry.getCurrentTenant().getConfig().getBoolean(CLONE_ORDER_STRATEGY_NORMALIZATION, false);
	}
}

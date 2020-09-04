/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package de.hybris.platform.order.strategies.ordercloning.impl.setup;

import static de.hybris.platform.order.strategies.ordercloning.impl.constants.EntryNumberFixConstants.PLATFORM_LOGO_CODE;

import de.hybris.platform.core.initialization.SystemSetup;

import java.io.InputStream;

import de.hybris.platform.order.strategies.ordercloning.impl.constants.EntryNumberFixConstants;
import de.hybris.platform.order.strategies.ordercloning.impl.service.EntryNumberFixService;


@SystemSetup(extension = EntryNumberFixConstants.EXTENSIONNAME)
public class EntryNumberFixSystemSetup
{
	private final EntryNumberFixService entryNumberFixService;

	public EntryNumberFixSystemSetup(final EntryNumberFixService entryNumberFixService)
	{
		this.entryNumberFixService = entryNumberFixService;
	}

	@SystemSetup(process = SystemSetup.Process.INIT, type = SystemSetup.Type.ESSENTIAL)
	public void createEssentialData()
	{
		entryNumberFixService.createLogo(PLATFORM_LOGO_CODE);
	}

	private InputStream getImageStream()
	{
		return EntryNumberFixSystemSetup.class.getResourceAsStream("/entryNumberFix/sap-hybris-platform.png");
	}
}

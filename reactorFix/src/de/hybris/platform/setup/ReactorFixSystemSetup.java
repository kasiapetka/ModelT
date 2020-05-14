/*
 * [y] hybris Platform
 *
 * Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package de.hybris.platform.setup;

import static de.hybris.platform.constants.ReactorFixConstants.PLATFORM_LOGO_CODE;

import de.hybris.platform.core.initialization.SystemSetup;

import java.io.InputStream;

import de.hybris.platform.constants.ReactorFixConstants;
import de.hybris.platform.service.ReactorFixService;


@SystemSetup(extension = ReactorFixConstants.EXTENSIONNAME)
public class ReactorFixSystemSetup
{
	private final ReactorFixService reactorFixService;

	public ReactorFixSystemSetup(final ReactorFixService reactorFixService)
	{
		this.reactorFixService = reactorFixService;
	}

	@SystemSetup(process = SystemSetup.Process.INIT, type = SystemSetup.Type.ESSENTIAL)
	public void createEssentialData()
	{
		reactorFixService.createLogo(PLATFORM_LOGO_CODE);
	}

	private InputStream getImageStream()
	{
		return ReactorFixSystemSetup.class.getResourceAsStream("/reactorFix/sap-hybris-platform.png");
	}
}

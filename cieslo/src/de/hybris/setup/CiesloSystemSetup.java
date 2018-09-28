/*
 * [y] hybris Platform
 *
 * Copyright (c) 2017 SAP SE or an SAP affiliate company.  All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package de.hybris.setup;

import static de.hybris.constants.CiesloConstants.PLATFORM_LOGO_CODE;

import de.hybris.platform.core.initialization.SystemSetup;

import java.io.InputStream;

import de.hybris.constants.CiesloConstants;
import de.hybris.service.CiesloService;


@SystemSetup(extension = CiesloConstants.EXTENSIONNAME)
public class CiesloSystemSetup
{
	private final CiesloService ciesloService;

	public CiesloSystemSetup(final CiesloService ciesloService)
	{
		this.ciesloService = ciesloService;
	}

	@SystemSetup(process = SystemSetup.Process.INIT, type = SystemSetup.Type.ESSENTIAL)
	public void createEssentialData()
	{
		ciesloService.createLogo(PLATFORM_LOGO_CODE);
	}

	private InputStream getImageStream()
	{
		return CiesloSystemSetup.class.getResourceAsStream("/cieslo/sap-hybris-platform.png");
	}
}

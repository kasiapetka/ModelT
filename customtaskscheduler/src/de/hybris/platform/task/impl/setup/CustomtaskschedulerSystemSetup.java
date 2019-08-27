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
package de.hybris.platform.task.impl.setup;

import static de.hybris.platform.task.impl.constants.CustomtaskschedulerConstants.PLATFORM_LOGO_CODE;

import de.hybris.platform.core.initialization.SystemSetup;

import java.io.InputStream;

import de.hybris.platform.task.impl.constants.CustomtaskschedulerConstants;
import de.hybris.platform.task.impl.service.CustomtaskschedulerService;


@SystemSetup(extension = CustomtaskschedulerConstants.EXTENSIONNAME)
public class CustomtaskschedulerSystemSetup
{
	private final CustomtaskschedulerService customtaskschedulerService;

	public CustomtaskschedulerSystemSetup(final CustomtaskschedulerService customtaskschedulerService)
	{
		this.customtaskschedulerService = customtaskschedulerService;
	}

	@SystemSetup(process = SystemSetup.Process.INIT, type = SystemSetup.Type.ESSENTIAL)
	public void createEssentialData()
	{
		customtaskschedulerService.createLogo(PLATFORM_LOGO_CODE);
	}

	private InputStream getImageStream()
	{
		return CustomtaskschedulerSystemSetup.class.getResourceAsStream("/customtaskscheduler/sap-hybris-platform.png");
	}
}

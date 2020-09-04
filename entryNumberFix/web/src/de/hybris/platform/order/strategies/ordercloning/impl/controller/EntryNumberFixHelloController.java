/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package de.hybris.platform.order.strategies.ordercloning.impl.controller;

import static de.hybris.platform.order.strategies.ordercloning.impl.constants.EntryNumberFixConstants.PLATFORM_LOGO_CODE;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import de.hybris.platform.order.strategies.ordercloning.impl.service.EntryNumberFixService;


@Controller
public class EntryNumberFixHelloController
{
	@Autowired
	private EntryNumberFixService entryNumberFixService;

	@RequestMapping(value = "/", method = RequestMethod.GET)
	public String printWelcome(final ModelMap model)
	{
		model.addAttribute("logoUrl", entryNumberFixService.getHybrisLogoUrl(PLATFORM_LOGO_CODE));
		return "welcome";
	}
}

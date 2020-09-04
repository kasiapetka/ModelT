/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package de.hybris.platform.order.strategies.ordercloning.impl.service;

public interface EntryNumberFixService
{
	String getHybrisLogoUrl(String logoCode);

	void createLogo(String logoCode);
}

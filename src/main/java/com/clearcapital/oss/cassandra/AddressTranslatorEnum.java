package com.clearcapital.oss.cassandra;

import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import com.datastax.driver.core.policies.IdentityTranslator;

/**
 * Provides serializable access to {@link AddressTranslator}
 */
public enum AddressTranslatorEnum {

    EC2MultiRegionAddressTranslator(new EC2MultiRegionAddressTranslator()),
    IdentityTranslator(new IdentityTranslator());

    private final AddressTranslator addressTranslator;
    AddressTranslatorEnum(AddressTranslator addressTranslator) {
        this.addressTranslator = addressTranslator;
    }

    /**
     *
     * @return Returns the address translator instance associated with the AddressTranslatorEnum
     */
    public AddressTranslator getAddressTranslator() {
        return addressTranslator;
    }
}

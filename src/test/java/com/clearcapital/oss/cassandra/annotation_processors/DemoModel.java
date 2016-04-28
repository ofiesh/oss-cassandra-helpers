package com.clearcapital.oss.cassandra.annotation_processors;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class DemoModel {

    public Long id;
    public Long updateId;
    public String fluidType;

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this, id, updateId, fluidType);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DemoModel) {
            DemoModel that = (DemoModel) obj;
            return Objects.equal(id, that.id) && Objects.equal(updateId, that.updateId)
                    && Objects.equal(fluidType, that.fluidType);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", id).add("updateId", updateId).add("fluidType", fluidType)
                .toString();
    }

    public static class Builder {

        DemoModel result;

        Builder() {
            result = new DemoModel();
        }

        public DemoModel build() {
            return result;
        }

        public Builder setId(Long value) {
            result.id = value;
            return this;
        }

        public Builder setUpdateId(Long value) {
            result.updateId = value;
            return this;
        }

        public Builder setFluidType(String value) {
            result.fluidType = value;
            return this;
        }
    }
}

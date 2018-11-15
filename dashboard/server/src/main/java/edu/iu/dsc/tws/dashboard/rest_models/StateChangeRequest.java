package edu.iu.dsc.tws.dashboard.rest_models;

import edu.iu.dsc.tws.dashboard.data_models.EntityState;

public class StateChangeRequest {

    private EntityState entityState;

    public EntityState getEntityState() {
        return entityState;
    }

    public void setEntityState(EntityState entityState) {
        this.entityState = entityState;
    }
}

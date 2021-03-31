package org.jlab.jaws;

import org.jlab.jaws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlarmStateCalculator {
    private static final Logger log = LoggerFactory.getLogger(AlarmStateCalculator.class);

    private AlarmStateCriteria criteria = new AlarmStateCriteria();

    public String computeState() {

        // Note: criteria are evaluated in increasing precedence order (last item, disabled, has highest precedence)

        // Should we have an Unregistered state or always default to Normal?
        AlarmState state = AlarmState.Normal;

        if(criteria.getActive()) {
            state = AlarmState.Active;
        }

        if(criteria.getOffDelayed()) {
            if(!criteria.getActive()) {  // Is this necessary?   If active then no need to incite active with OffDelay
                state = AlarmState.OffDelayed;
            }
        }

        if(criteria.getLatched()) {
            if(criteria.getActive()) {
                state = AlarmState.Latched;
            } else {
                state = AlarmState.InactiveLatched;
            }
        }

        if(criteria.getOnDelayed()) {
            if(criteria.getActive()) {  // Is this necessary?   If not active then no need to suppress active with OnDelay
                state = AlarmState.OnDelayed;
            }
        }

        if(criteria.getShelved()) {
            if(criteria.getOneshot()) {
                state = AlarmState.OneShotShelved;
            } else {
                if(criteria.getActive()) {
                    state = AlarmState.ContinuousShelved;
                } else {
                    state = AlarmState.InactiveContinuousShelved;
                }
            }
        }

        if(criteria.getDisabled()) {
            if(criteria.getActive()) {
                state = AlarmState.Disabled;
            } else {
                state = AlarmState.InactiveDisabled;
            }
        }

        log.info("Computing State for: {}", criteria.getName());
        log.info("Registered:      {}", criteria.getRegistered());
        log.info("Active:          {}", criteria.getActive());
        log.info("OffDelayed:      {}", criteria.getOffDelayed());
        log.info("Latched:         {}", criteria.getLatched());
        log.info("OnDelayed:       {}", criteria.getOnDelayed());
        log.info("Shelved:         {}", criteria.getShelved());
        log.info("Disabled:        {}", criteria.getDisabled());
        log.info("Effective State: {}", state.name());

        return state.name();
    }

    public void append(AlarmStateCriteria criteria) {
        if(criteria.getName() != null) {
            this.criteria.setName(criteria.getName());
        }
        if(criteria.getRegistered()) {
            this.criteria.setRegistered(true);
        }
        if(criteria.getActive()) {
            this.criteria.setActive(true);
        }
        if(criteria.getOffDelayed()) {
            this.criteria.setOffDelayed(true);
        }
        if(criteria.getLatched()) {
            this.criteria.setLatched(true);
        }
        if(criteria.getOnDelayed()) {
            this.criteria.setOnDelayed(true);
        }
        if(criteria.getShelved()) {
            this.criteria.setShelved(true);
        }
        if(criteria.getDisabled()) {
            this.criteria.setDisabled(true);
        }

    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        builder.append(criteria.getName());
        builder.append(",");
        builder.append(criteria.getRegistered());
        builder.append(",");
        builder.append(criteria.getActive());
        builder.append(",");
        builder.append(criteria.getOffDelayed());
        builder.append(",");
        builder.append(criteria.getLatched());
        builder.append(",");
        builder.append(criteria.getOffDelayed());
        builder.append(",");
        builder.append(criteria.getShelved());
        builder.append(",");
        builder.append(criteria.getDisabled());
        builder.append(",");
        builder.append(computeState());
        builder.append("]");

        return builder.toString();
    }

    public AlarmStateCriteria getCriteria() {
        return criteria;
    }
}

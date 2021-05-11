package org.jlab.jaws;

import org.jlab.jaws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlarmStateCalculator {
    private static final Logger log = LoggerFactory.getLogger(AlarmStateCalculator.class);

    private AlarmStateCriteria criteria = new AlarmStateCriteria();

    public AlarmStateEnum computeState() {

        // Note: criteria are evaluated in increasing precedence order (last item, disabled, has highest precedence)

        // Should we have an Unregistered state or always default to Normal?
        AlarmStateEnum state = AlarmStateEnum.Normal;

        if(criteria.getActive()) {
            state = AlarmStateEnum.Active;
        }

        if(criteria.getOffDelayed()) {
            if(!criteria.getActive()) {  // Is this necessary?   If active then no need to incite active with OffDelay
                state = AlarmStateEnum.OffDelayed;
            }
        }

        if(criteria.getLatched()) {
            if(criteria.getActive()) {
                state = AlarmStateEnum.Latched;
            } else {
                state = AlarmStateEnum.NormalLatched;
            }
        }

        if(criteria.getOnDelayed()) {
            if(criteria.getActive()) {  // Is this necessary?   If not active then no need to suppress active with OnDelay
                state = AlarmStateEnum.OnDelayed;
            }
        }

        if(criteria.getContinuousShelved()) {
            if(criteria.getActive()) {
                state = AlarmStateEnum.ContinuousShelved;
            } else {
                state = AlarmStateEnum.NormalContinuousShelved;
            }
        }

        if(criteria.getOneshotShelved()) {
            if(criteria.getActive()) { // Once no longer active the "one shot" is used up, right?
                state = AlarmStateEnum.OneShotShelved;
            }
        }

        if(criteria.getMasked()) {
            if(criteria.getActive()) { // Once no longer active then no longer masked, right?
                state = AlarmStateEnum.Masked;
            }
        }

        if(criteria.getFiltered()) {
            if(criteria.getActive()) {
                state = AlarmStateEnum.Filtered;
            } else {
                state = AlarmStateEnum.NormalFiltered;
            }
        }

        if(criteria.getDisabled()) {
            if(criteria.getActive()) {
                state = AlarmStateEnum.Disabled;
            } else {
                state = AlarmStateEnum.NormalDisabled;
            }
        }

        log.info("Computing State for: {}", criteria.getName());
        log.info("Registered:          {}", criteria.getRegistered());
        log.info("Active:              {}", criteria.getActive());
        log.info("OffDelayed:          {}", criteria.getOffDelayed());
        log.info("Latched:             {}", criteria.getLatched());
        log.info("ContinuousShelved:   {}", criteria.getContinuousShelved());
        log.info("OneshotShelved:      {}", criteria.getOneshotShelved());
        log.info("OnDelayed:           {}", criteria.getOnDelayed());
        log.info("Masked:              {}", criteria.getMasked());
        log.info("Filtered:            {}", criteria.getFiltered());
        log.info("Disabled:            {}", criteria.getDisabled());
        log.info("Effective State:     {}", state.name());

        return state;
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
        if(criteria.getContinuousShelved()) {
            this.criteria.setContinuousShelved(true);
        }
        if(criteria.getOneshotShelved()) {
            this.criteria.setOneshotShelved(true);
        }
        if(criteria.getOnDelayed()) {
            this.criteria.setOnDelayed(true);
        }
        if(criteria.getMasked()) {
            this.criteria.setMasked(true);
        }
        if(criteria.getFiltered()) {
            this.criteria.setFiltered(true);
        }
        if(criteria.getDisabled()) {
            this.criteria.setDisabled(true);
        }
    }

    public void remove(AlarmStateCriteria criteria) {
        if(criteria.getName() != null) {
            this.criteria.setName(null);
        }
        if(criteria.getRegistered()) {
            this.criteria.setRegistered(false);
        }
        if(criteria.getActive()) {
            this.criteria.setActive(false);
        }
        if(criteria.getOffDelayed()) {
            this.criteria.setOffDelayed(false);
        }
        if(criteria.getLatched()) {
            this.criteria.setLatched(false);
        }
        if(criteria.getContinuousShelved()) {
            this.criteria.setContinuousShelved(false);
        }
        if(criteria.getOneshotShelved()) {
            this.criteria.setOneshotShelved(false);
        }
        if(criteria.getOnDelayed()) {
            this.criteria.setOnDelayed(false);
        }
        if(criteria.getMasked()) {
            this.criteria.setMasked(false);
        }
        if(criteria.getFiltered()) {
            this.criteria.setFiltered(false);
        }
        if(criteria.getDisabled()) {
            this.criteria.setDisabled(false);
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
        builder.append(criteria.getContinuousShelved());
        builder.append(",");
        builder.append(criteria.getOneshotShelved());
        builder.append(",");
        builder.append(criteria.getOnDelayed());
        builder.append(",");
        builder.append(criteria.getMasked());
        builder.append(",");
        builder.append(criteria.getFiltered());
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

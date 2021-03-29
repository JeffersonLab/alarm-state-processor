package org.jlab.jaws;

import org.jlab.jaws.entity.ActiveAlarm;
import org.jlab.jaws.entity.DisabledAlarm;
import org.jlab.jaws.entity.RegisteredAlarm;
import org.jlab.jaws.entity.ShelvedAlarm;

public class AlarmStateCalculator {
    public RegisteredAlarm registeredAlarm;
    public ActiveAlarm activeAlarm;
    public DisabledAlarm disabledAlarm;
    public ShelvedAlarm shelvedAlarm;

    public static AlarmStateCalculator fromRegisteredAndActive(RegisteredAlarm registeredAlarm, ActiveAlarm activeAlarm) {
        AlarmStateCalculator state = new AlarmStateCalculator();

        state.registeredAlarm = registeredAlarm;
        state.activeAlarm = activeAlarm;

        return state;
    }

    public AlarmStateCalculator addDisabled(DisabledAlarm disabledAlarm) {
        this.disabledAlarm = disabledAlarm;

        return this;
    }

    public AlarmStateCalculator addShelved(ShelvedAlarm shelvedAlarm) {
        this.shelvedAlarm = shelvedAlarm;

        return this;
    }

    public String computeState() {
        String state = "Normal";

        if(activeAlarm != null) {
            state = "Active";
        }

        if(shelvedAlarm != null) {
            state = "Shelved";
        }

        if(disabledAlarm != null) {
            state = "Disabled";
        }

        return state;
    }
}

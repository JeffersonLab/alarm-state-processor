package org.jlab.jaws;

import org.jlab.jaws.entity.ActiveAlarm;
import org.jlab.jaws.entity.RegisteredAlarm;
import org.jlab.jaws.entity.ShelvedAlarm;

public class AlarmState {
    public RegisteredAlarm registeredAlarm;
    public ShelvedAlarm shelvedAlarm;
    public ActiveAlarm activeAlarm;

    public static AlarmState fromRegisteredAndActive(RegisteredAlarm registeredAlarm, ActiveAlarm activeAlarm) {
        AlarmState state = new AlarmState();

        state.registeredAlarm = registeredAlarm;
        state.activeAlarm = activeAlarm;

        return state;
    }

    public AlarmState addShelved(ShelvedAlarm shelvedAlarm) {
        this.shelvedAlarm = shelvedAlarm;

        return this;
    }

    public String computeState() {
        String state = "Normal";

        if(activeAlarm != null) {
            state = "Active";
        }

        return state;
    }
}

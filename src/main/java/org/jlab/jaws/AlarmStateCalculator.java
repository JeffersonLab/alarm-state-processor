package org.jlab.jaws;

import org.jlab.jaws.entity.*;

public class AlarmStateCalculator {
    public RegisteredAlarm registeredAlarm;
    public LatchedAlarm latchedAlarm;
    public ActiveAlarm activeAlarm;
    public DisabledAlarm disabledAlarm;
    public ShelvedAlarm shelvedAlarm;

    public static AlarmStateCalculator fromRegisteredAndActive(RegisteredAlarm registeredAlarm, ActiveAlarm activeAlarm) {
        AlarmStateCalculator state = new AlarmStateCalculator();

        state.registeredAlarm = registeredAlarm;
        state.activeAlarm = activeAlarm;

        return state;
    }

    public AlarmStateCalculator addLatched(LatchedAlarm latchedAlarm) {
        this.latchedAlarm = latchedAlarm;

        return this;
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

        System.err.println("Computing State");
        System.err.println("Registered: " + registeredAlarm == null);
        System.err.println("Active:     " + activeAlarm == null);
        System.err.println("Latched:    " + latchedAlarm == null);
        System.err.println("Shelved:    " + shelvedAlarm == null);
        System.err.println("Disabled:   " + disabledAlarm == null);

        AlarmState state = AlarmState.Normal;

        if(activeAlarm != null) {
            state = AlarmState.Active;
        }

        if(latchedAlarm != null) {
            if(activeAlarm != null) {
                state = AlarmState.Latched;
            } else {
                state = AlarmState.InactiveLatched;
            }
        }

        if(shelvedAlarm != null) {
            if(shelvedAlarm.getOneshot()) {
                state = AlarmState.OneShotShelved;
            } else {
                if(activeAlarm != null) {
                    state = AlarmState.ContinuousShelved;
                } else {
                    state = AlarmState.InactiveContinuousShelved;
                }
            }
        }

        if(disabledAlarm != null) {
            if(activeAlarm != null) {
                state = AlarmState.Disabled;
            } else {
                state = AlarmState.InactiveDisabled;
            }
        }

        return state.name();
    }
}

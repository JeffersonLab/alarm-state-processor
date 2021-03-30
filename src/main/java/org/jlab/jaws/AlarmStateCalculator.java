package org.jlab.jaws;

import org.jlab.jaws.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlarmStateCalculator {
    private static final Logger log = LoggerFactory.getLogger(AlarmStateCalculator.class);

    private RegisteredAlarm registeredAlarm;
    private LatchedAlarm latchedAlarm;
    private ActiveAlarm activeAlarm;
    private DisabledAlarm disabledAlarm;
    private ShelvedAlarm shelvedAlarm;
    private String alarmName;

    public static AlarmStateCalculator fromRegisteredAndActive(RegisteredAlarm registeredAlarm, ActiveAlarm activeAlarm) {
        AlarmStateCalculator state = new AlarmStateCalculator();

        state.registeredAlarm = registeredAlarm;
        state.activeAlarm = activeAlarm;

        return state;
    }

    public void setAlarmName(String alarmName) {
        this.alarmName = alarmName;
    }

    public AlarmStateCalculator setLatched(LatchedAlarm latchedAlarm) {
        this.latchedAlarm = latchedAlarm;

        return this;
    }

    public AlarmStateCalculator setDisabled(DisabledAlarm disabledAlarm) {
        this.disabledAlarm = disabledAlarm;

        return this;
    }

    public AlarmStateCalculator setShelved(ShelvedAlarm shelvedAlarm) {
        this.shelvedAlarm = shelvedAlarm;

        return this;
    }

    public String computeState() {

        log.info("Computing State for: {}", alarmName);
        log.info("Registered: {}", registeredAlarm != null);
        log.info("Active:     {}", activeAlarm != null);
        log.info("Latched:    {}", latchedAlarm != null);
        log.info("Shelved:    {}", shelvedAlarm != null);
        log.info("Disabled:   {}", disabledAlarm != null);

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

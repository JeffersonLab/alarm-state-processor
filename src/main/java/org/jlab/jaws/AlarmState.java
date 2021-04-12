package org.jlab.jaws;

/**
 * The possible alarm states.
 *
 * The states are ordered such that larger offsets have higher precedence as there is only one effective state at a
 * time, though alarms could have multiple overrides in effect simultaneously.
 *
 * Some states are mutually exclusive such as variants indicating Inactive or Oneshot vs Continuous Shelved.
 *
 * The set of overrides is a subset of the set of possible alarm states as the states combine active and override and
 * explicitly key on one shot vs continuous shelving.
 *
 */
public enum AlarmState {
    Normal,
    Active,
    Latched,
    NormalLatched,
    OffDelayed,
    ContinuousShelved,
    NormalContinuousShelved,
    OneShotShelved,
    OnDelayed,
    Masked,
    Filtered,
    NormalFiltered,
    Disabled,
    NormalDisabled
}

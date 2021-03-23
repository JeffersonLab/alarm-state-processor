package org.jlab.alarms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Set;

public class FilterCommandValue {
    private String filterName;
    private Set<String> alarmNames;
    private Set<String> locations;
    private Set<String> categories;

    public FilterCommandValue() {

    }

    public Set<String> getAlarmNames() {
        return alarmNames;
    }

    public void setAlarmNames(Set<String> alarmNames) {
        this.alarmNames = alarmNames;
    }

    public Set<String> getLocations() {
        return locations;
    }

    public void setLocations(Set<String> locations) {
        this.locations = locations;
    }

    public Set<String> getCategories() {
        return categories;
    }

    public void setCategories(Set<String> categories) {
        this.categories = categories;
    }

    public String getFilterName() {
        return filterName;
    }

    public void setFilterName(String filterName) {
        this.filterName = filterName;
    }

    public String toString() {
        ObjectMapper objectMapper = new ObjectMapper();

        String json = null;
        try {
            json = objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return json;
    }
}

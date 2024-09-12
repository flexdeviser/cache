package org.e4s.model;

import java.io.Serializable;

public class PQ implements Serializable {

    private static final long serialVersionUID = 150605571008276150L;

    private String key;

    private long effectiveTs;

    private float voltageA;
    private float voltageB;
    private float voltageC;


    private float current;

    private float activePowerA;
    private float activePowerB;
    private float activePowerC;


    private float inactivePowerA;
    private float inactivePowerB;
    private float inactivePowerC;

    private int[] histogram;

    public PQ() {
    }

    public PQ(String key, long effectiveTs) {
        this.key = key;
        this.effectiveTs = effectiveTs;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getEffectiveTs() {
        return effectiveTs;
    }

    public void setEffectiveTs(long effectiveTs) {
        this.effectiveTs = effectiveTs;
    }


    public float getVoltageA() {
        return voltageA;
    }

    public void setVoltageA(float voltageA) {
        this.voltageA = voltageA;
    }

    public float getVoltageB() {
        return voltageB;
    }

    public void setVoltageB(float voltageB) {
        this.voltageB = voltageB;
    }

    public float getVoltageC() {
        return voltageC;
    }

    public void setVoltageC(float voltageC) {
        this.voltageC = voltageC;
    }

    public float getCurrent() {
        return current;
    }

    public void setCurrent(float current) {
        this.current = current;
    }

    public float getActivePowerA() {
        return activePowerA;
    }

    public void setActivePowerA(float activePowerA) {
        this.activePowerA = activePowerA;
    }

    public float getActivePowerB() {
        return activePowerB;
    }

    public void setActivePowerB(float activePowerB) {
        this.activePowerB = activePowerB;
    }

    public float getActivePowerC() {
        return activePowerC;
    }

    public void setActivePowerC(float activePowerC) {
        this.activePowerC = activePowerC;
    }

    public float getInactivePowerA() {
        return inactivePowerA;
    }

    public void setInactivePowerA(float inactivePowerA) {
        this.inactivePowerA = inactivePowerA;
    }

    public float getInactivePowerB() {
        return inactivePowerB;
    }

    public void setInactivePowerB(float inactivePowerB) {
        this.inactivePowerB = inactivePowerB;
    }

    public float getInactivePowerC() {
        return inactivePowerC;
    }

    public void setInactivePowerC(float inactivePowerC) {
        this.inactivePowerC = inactivePowerC;
    }

    public int[] getHistogram() {
        return histogram;
    }

    public void setHistogram(int[] histogram) {
        this.histogram = histogram;
    }

    @Override
    public String toString() {
        return "PQ{" +
            "key='" + key + '\'' +
            ", effectiveTs=" + effectiveTs +
            '}';
    }
}

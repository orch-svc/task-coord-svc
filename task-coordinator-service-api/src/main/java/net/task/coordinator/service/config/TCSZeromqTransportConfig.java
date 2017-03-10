package net.task.coordinator.service.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TCSZeromqTransportConfig {

    @JsonProperty("brokerAddress")
    private String ipAddress;
    private String frontendPort;
    private String backendPort;
    private String frontendProtocol;
    private String backendProtocol;
    private boolean autoStart;

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getFrontendPort() {
        return frontendPort;
    }

    public void setFrontendPort(String frontendPort) {
        this.frontendPort = frontendPort;
    }

    public String getBackendPort() {
        return backendPort;
    }

    public void setBackendPort(String backendPort) {
        this.backendPort = backendPort;
    }

    public String getFrontendProtocol() {
        return frontendProtocol;
    }

    public void setFrontendProtocol(String frontendProtocol) {
        this.frontendProtocol = frontendProtocol;
    }

    public String getBackendProtocol() {
        return backendProtocol;
    }

    public void setBackendProtocol(String backendProtocol) {
        this.backendProtocol = backendProtocol;
    }

    public boolean isAutoStart() {
        return autoStart;
    }

    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (autoStart ? 1231 : 1237);
        result = prime * result + ((backendPort == null) ? 0 : backendPort.hashCode());
        result = prime * result + ((backendProtocol == null) ? 0 : backendProtocol.hashCode());
        result = prime * result + ((frontendPort == null) ? 0 : frontendPort.hashCode());
        result = prime * result + ((frontendProtocol == null) ? 0 : frontendProtocol.hashCode());
        result = prime * result + ((ipAddress == null) ? 0 : ipAddress.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TCSZeromqTransportConfig other = (TCSZeromqTransportConfig) obj;
        if (autoStart != other.autoStart)
            return false;
        if (backendPort == null) {
            if (other.backendPort != null)
                return false;
        } else if (!backendPort.equals(other.backendPort))
            return false;
        if (backendProtocol == null) {
            if (other.backendProtocol != null)
                return false;
        } else if (!backendProtocol.equals(other.backendProtocol))
            return false;
        if (frontendPort == null) {
            if (other.frontendPort != null)
                return false;
        } else if (!frontendPort.equals(other.frontendPort))
            return false;
        if (frontendProtocol == null) {
            if (other.frontendProtocol != null)
                return false;
        } else if (!frontendProtocol.equals(other.frontendProtocol))
            return false;
        if (ipAddress == null) {
            if (other.ipAddress != null)
                return false;
        } else if (!ipAddress.equals(other.ipAddress))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "TCSzeromqTransportConfig [ipAddress=" + ipAddress + ", frontendPort=" + frontendPort + ", backendPort="
                + backendPort + ", frontendProtocol=" + frontendProtocol + ", backendProtocol=" + backendProtocol
                + ", autoStart=" + autoStart + "]";
    }

}

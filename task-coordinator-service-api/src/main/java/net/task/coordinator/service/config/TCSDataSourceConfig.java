package net.task.coordinator.service.config;

public class TCSDataSourceConfig {
    public String getDbConnectString() {
        return dbConnectString;
    }

    public void setDbConnectString(String dbConnectString) {
        this.dbConnectString = dbConnectString;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return "DBConfig [dbConnectString=" + dbConnectString + ", userName=" + userName + ", password=" + password
                + "]";
    }

    private String dbConnectString;
    private String userName;
    private String password;
}

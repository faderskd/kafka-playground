package kafkaplayground.producer;

public record AuditLog(long timestamp, String userId, ActionType actionType, String deviceId) {
    public enum ActionType {
        USER_SIGNUP,
        USER_LOGIN,
        USER_LOGOUT,
        PASSWORD_CHANGE
    }
}

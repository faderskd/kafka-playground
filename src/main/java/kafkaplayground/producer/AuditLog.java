package kafkaplayground.producer;

public record AuditLog(String timestamp, String username, ActionType actionType) {
    public enum ActionType {
        USER_SIGNUP,
        USER_LOGIN,
        USER_LOGOUT,
        PASSWORD_CHANGE
    }
}

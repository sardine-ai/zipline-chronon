package ai.chronon.service.test;

public enum OrderStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    CANCELLED;

    public static OrderStatus fromString(String value) {
        try {
            return valueOf(value.toUpperCase());
        } catch (Exception e) {
            return null;
        }
    }
}

package ai.chronon.service.test;

import ai.chronon.api.Accuracy;

import java.util.List;
import java.util.Map;

public class TestInput {
    private String userId;
    private OrderStatus status;  // bare enum
    private UserRole role;       // enum with custom fromString
    private Accuracy accuracy;   // thrift enum
    private int limit;
    private double amount;
    private boolean active;
    private List<String> items;  // list type
    private Map<String, String> props;  // map type

    public TestInput() {}

    // Setters
    public void setUserId(String userId) { this.userId = userId; }
    public void setStatus(OrderStatus status) { this.status = status; }
    public void setRole(UserRole role) { this.role = role; }
    public void setAccuracy(Accuracy accuracy) { this.accuracy = accuracy; }
    public void setLimit(int limit) { this.limit = limit; }
    public void setAmount(double amount) { this.amount = amount; }
    public void setActive(boolean active) { this.active = active; }
    public void setItems(List<String> items) { this.items = items; }
    public void setProps(Map<String, String> props) { this.props = props; }

    // Getters
    public String getUserId() { return userId; }
    public OrderStatus getStatus() { return status; }
    public UserRole getRole() { return role; }
    public Accuracy getAccuracy() { return accuracy; }
    public int getLimit() { return limit; }
    public double getAmount() { return amount; }
    public boolean isActive() { return active; }
    public List<String> getItems() { return items; }
    public Map<String, String> getProps() { return props; }
}

package com.zomato.pipeline1.events;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * POJO representing an order event consumed from Kafka.
 * Used as the input type for CEP pattern matching in Pipeline 1.
 *
 * <p>This class follows the builder pattern for convenient construction
 * and implements proper equals/hashCode semantics keyed on orderId + updatedAt
 * so that duplicate detection works correctly across windows.</p>
 */
public class OrderEvent implements Serializable {

    private static final long serialVersionUID = 2L;

    // -----------------------------------------------------------------------
    // Order Status Enum
    // -----------------------------------------------------------------------

    public enum OrderStatus {
        PLACED,
        CONFIRMED,
        PREPARING,
        READY_FOR_PICKUP,
        DISPATCHED,
        IN_TRANSIT,
        DELIVERED,
        CANCELLED,
        REFUNDED,
        FAILED,
        PARTIALLY_DELIVERED;

        /**
         * Parses a status string case-insensitively, returning {@code null} for unknown values.
         */
        public static OrderStatus fromString(String value) {
            if (value == null || value.isEmpty()) {
                return null;
            }
            try {
                return OrderStatus.valueOf(value.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }

        public boolean isTerminal() {
            return this == DELIVERED || this == CANCELLED || this == REFUNDED || this == FAILED;
        }

        public boolean isCancellation() {
            return this == CANCELLED || this == REFUNDED;
        }
    }

    // -----------------------------------------------------------------------
    // Event Type Enum
    // -----------------------------------------------------------------------

    public enum EventType {
        ORDER_CREATED,
        ORDER_UPDATED,
        ORDER_STATUS_CHANGED,
        ORDER_AMOUNT_MODIFIED,
        ORDER_ADDRESS_CHANGED,
        ORDER_PAYMENT_UPDATED,
        ORDER_PROMO_APPLIED,
        ORDER_ITEM_ADDED,
        ORDER_ITEM_REMOVED,
        ORDER_CANCELLED,
        ORDER_DELIVERED;

        public static EventType fromString(String value) {
            if (value == null || value.isEmpty()) {
                return null;
            }
            try {
                return EventType.valueOf(value.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Fields
    // -----------------------------------------------------------------------

    private String orderId;
    private String userId;
    private String restaurantId;
    private BigDecimal orderAmount;
    private OrderStatus orderStatus;
    private String deliveryAddress;
    private String city;
    private String zone;
    private String paymentMethod;
    private int itemCount;
    private String promoCode;
    private long timestamp;
    private EventType eventType;

    // Extended fields retained from original schema
    private String status;
    private BigDecimal subtotal;
    private BigDecimal tax;
    private BigDecimal deliveryFee;
    private BigDecimal totalAmount;
    private double latitude;
    private double longitude;
    private long createdAt;
    private long updatedAt;
    private String opType;

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String userId, String restaurantId, String status,
                      BigDecimal totalAmount, String city, String deliveryAddress,
                      long createdAt, long updatedAt) {
        this.orderId = orderId;
        this.userId = userId;
        this.restaurantId = restaurantId;
        this.status = status;
        this.orderStatus = OrderStatus.fromString(status);
        this.totalAmount = totalAmount;
        this.orderAmount = totalAmount;
        this.city = city;
        this.deliveryAddress = deliveryAddress;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.timestamp = updatedAt;
    }

    // -----------------------------------------------------------------------
    // Builder
    // -----------------------------------------------------------------------

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final OrderEvent event = new OrderEvent();

        private Builder() {
        }

        public Builder orderId(String orderId) {
            event.orderId = orderId;
            return this;
        }

        public Builder userId(String userId) {
            event.userId = userId;
            return this;
        }

        public Builder restaurantId(String restaurantId) {
            event.restaurantId = restaurantId;
            return this;
        }

        public Builder orderAmount(BigDecimal orderAmount) {
            event.orderAmount = orderAmount;
            event.totalAmount = orderAmount;
            return this;
        }

        public Builder orderStatus(OrderStatus orderStatus) {
            event.orderStatus = orderStatus;
            event.status = orderStatus != null ? orderStatus.name() : null;
            return this;
        }

        public Builder deliveryAddress(String deliveryAddress) {
            event.deliveryAddress = deliveryAddress;
            return this;
        }

        public Builder city(String city) {
            event.city = city;
            return this;
        }

        public Builder zone(String zone) {
            event.zone = zone;
            return this;
        }

        public Builder paymentMethod(String paymentMethod) {
            event.paymentMethod = paymentMethod;
            return this;
        }

        public Builder itemCount(int itemCount) {
            event.itemCount = itemCount;
            return this;
        }

        public Builder promoCode(String promoCode) {
            event.promoCode = promoCode;
            return this;
        }

        public Builder timestamp(long timestamp) {
            event.timestamp = timestamp;
            event.updatedAt = timestamp;
            return this;
        }

        public Builder eventType(EventType eventType) {
            event.eventType = eventType;
            return this;
        }

        public Builder subtotal(BigDecimal subtotal) {
            event.subtotal = subtotal;
            return this;
        }

        public Builder tax(BigDecimal tax) {
            event.tax = tax;
            return this;
        }

        public Builder deliveryFee(BigDecimal deliveryFee) {
            event.deliveryFee = deliveryFee;
            return this;
        }

        public Builder latitude(double latitude) {
            event.latitude = latitude;
            return this;
        }

        public Builder longitude(double longitude) {
            event.longitude = longitude;
            return this;
        }

        public Builder createdAt(long createdAt) {
            event.createdAt = createdAt;
            return this;
        }

        public Builder opType(String opType) {
            event.opType = opType;
            return this;
        }

        public OrderEvent build() {
            Objects.requireNonNull(event.orderId, "orderId must not be null");
            Objects.requireNonNull(event.userId, "userId must not be null");
            return event;
        }
    }

    // -----------------------------------------------------------------------
    // Getters and setters
    // -----------------------------------------------------------------------

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getRestaurantId() {
        return restaurantId;
    }

    public void setRestaurantId(String restaurantId) {
        this.restaurantId = restaurantId;
    }

    public BigDecimal getOrderAmount() {
        return orderAmount;
    }

    public void setOrderAmount(BigDecimal orderAmount) {
        this.orderAmount = orderAmount;
    }

    public OrderStatus getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(OrderStatus orderStatus) {
        this.orderStatus = orderStatus;
        this.status = orderStatus != null ? orderStatus.name() : null;
    }

    public String getDeliveryAddress() {
        return deliveryAddress;
    }

    public void setDeliveryAddress(String deliveryAddress) {
        this.deliveryAddress = deliveryAddress;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public int getItemCount() {
        return itemCount;
    }

    public void setItemCount(int itemCount) {
        this.itemCount = itemCount;
    }

    public String getPromoCode() {
        return promoCode;
    }

    public void setPromoCode(String promoCode) {
        this.promoCode = promoCode;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        this.orderStatus = OrderStatus.fromString(status);
    }

    public BigDecimal getSubtotal() {
        return subtotal;
    }

    public void setSubtotal(BigDecimal subtotal) {
        this.subtotal = subtotal;
    }

    public BigDecimal getTax() {
        return tax;
    }

    public void setTax(BigDecimal tax) {
        this.tax = tax;
    }

    public BigDecimal getDeliveryFee() {
        return deliveryFee;
    }

    public void setDeliveryFee(BigDecimal deliveryFee) {
        this.deliveryFee = deliveryFee;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

    // -----------------------------------------------------------------------
    // Derived helpers
    // -----------------------------------------------------------------------

    /**
     * Returns the event timestamp as an {@link Instant} (millisecond epoch).
     */
    public Instant getEventTime() {
        return Instant.ofEpochMilli(timestamp > 0 ? timestamp : updatedAt);
    }

    /**
     * Returns true if this event represents a cancellation.
     */
    public boolean isCancellation() {
        return orderStatus != null && orderStatus.isCancellation()
                || "CANCELLED".equalsIgnoreCase(status);
    }

    /**
     * Returns true if the order has reached a terminal state.
     */
    public boolean isTerminal() {
        return orderStatus != null && orderStatus.isTerminal();
    }

    /**
     * Returns the effective monetary amount, preferring orderAmount over totalAmount.
     */
    public BigDecimal getEffectiveAmount() {
        return orderAmount != null ? orderAmount : totalAmount;
    }

    // -----------------------------------------------------------------------
    // equals / hashCode / toString
    // -----------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderEvent that = (OrderEvent) o;
        return Objects.equals(orderId, that.orderId)
                && updatedAt == that.updatedAt;
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, updatedAt);
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", restaurantId='" + restaurantId + '\'' +
                ", orderStatus=" + orderStatus +
                ", orderAmount=" + orderAmount +
                ", city='" + city + '\'' +
                ", zone='" + zone + '\'' +
                ", deliveryAddress='" + deliveryAddress + '\'' +
                ", paymentMethod='" + paymentMethod + '\'' +
                ", itemCount=" + itemCount +
                ", promoCode='" + promoCode + '\'' +
                ", eventType=" + eventType +
                ", timestamp=" + timestamp +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }
}

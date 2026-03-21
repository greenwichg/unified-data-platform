package com.zomato.pipeline1.events;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * POJO representing a menu event consumed from Kafka.
 * Captures changes to restaurant menu items for CEP processing.
 *
 * <p>This class includes a builder pattern for ergonomic construction,
 * proper equals/hashCode keyed on (menuId, lastUpdated), and derived
 * helpers for price-change and availability analysis.</p>
 */
public class MenuEvent implements Serializable {

    private static final long serialVersionUID = 2L;

    // -----------------------------------------------------------------------
    // Event Type Enum
    // -----------------------------------------------------------------------

    public enum EventType {
        MENU_ITEM_CREATED,
        MENU_ITEM_UPDATED,
        MENU_ITEM_PRICE_CHANGED,
        MENU_ITEM_AVAILABILITY_CHANGED,
        MENU_ITEM_DELETED,
        MENU_ITEM_CATEGORY_CHANGED,
        MENU_ITEM_IMAGE_UPDATED;

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

    // Core fields as specified
    private String menuId;
    private String restaurantId;
    private String itemName;
    private String category;
    private String cuisine;
    private BigDecimal price;
    private boolean isAvailable;
    private long lastUpdated;
    private EventType eventType;

    // Extended fields retained from original schema
    private String itemId;
    private String name;
    private String cuisineType;
    private BigDecimal previousPrice;
    private boolean vegetarian;
    private boolean vegan;
    private boolean available;
    private int preparationTimeMins;
    private float rating;
    private int numRatings;
    private String imageUrl;
    private String description;
    private long createdAt;
    private long updatedAt;
    private String opType;

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    public MenuEvent() {
    }

    public MenuEvent(String itemId, String restaurantId, String name,
                     String category, BigDecimal price, boolean available,
                     long updatedAt) {
        this.itemId = itemId;
        this.menuId = itemId;
        this.restaurantId = restaurantId;
        this.name = name;
        this.itemName = name;
        this.category = category;
        this.price = price;
        this.available = available;
        this.isAvailable = available;
        this.updatedAt = updatedAt;
        this.lastUpdated = updatedAt;
    }

    // -----------------------------------------------------------------------
    // Builder
    // -----------------------------------------------------------------------

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final MenuEvent event = new MenuEvent();

        private Builder() {
        }

        public Builder menuId(String menuId) {
            event.menuId = menuId;
            event.itemId = menuId;
            return this;
        }

        public Builder restaurantId(String restaurantId) {
            event.restaurantId = restaurantId;
            return this;
        }

        public Builder itemName(String itemName) {
            event.itemName = itemName;
            event.name = itemName;
            return this;
        }

        public Builder category(String category) {
            event.category = category;
            return this;
        }

        public Builder cuisine(String cuisine) {
            event.cuisine = cuisine;
            event.cuisineType = cuisine;
            return this;
        }

        public Builder price(BigDecimal price) {
            event.price = price;
            return this;
        }

        public Builder previousPrice(BigDecimal previousPrice) {
            event.previousPrice = previousPrice;
            return this;
        }

        public Builder isAvailable(boolean isAvailable) {
            event.isAvailable = isAvailable;
            event.available = isAvailable;
            return this;
        }

        public Builder lastUpdated(long lastUpdated) {
            event.lastUpdated = lastUpdated;
            event.updatedAt = lastUpdated;
            return this;
        }

        public Builder eventType(EventType eventType) {
            event.eventType = eventType;
            return this;
        }

        public Builder vegetarian(boolean vegetarian) {
            event.vegetarian = vegetarian;
            return this;
        }

        public Builder vegan(boolean vegan) {
            event.vegan = vegan;
            return this;
        }

        public Builder preparationTimeMins(int preparationTimeMins) {
            event.preparationTimeMins = preparationTimeMins;
            return this;
        }

        public Builder rating(float rating) {
            event.rating = rating;
            return this;
        }

        public Builder numRatings(int numRatings) {
            event.numRatings = numRatings;
            return this;
        }

        public Builder imageUrl(String imageUrl) {
            event.imageUrl = imageUrl;
            return this;
        }

        public Builder description(String description) {
            event.description = description;
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

        public MenuEvent build() {
            Objects.requireNonNull(event.menuId != null ? event.menuId : event.itemId,
                    "menuId (itemId) must not be null");
            Objects.requireNonNull(event.restaurantId, "restaurantId must not be null");
            return event;
        }
    }

    // -----------------------------------------------------------------------
    // Getters and setters
    // -----------------------------------------------------------------------

    public String getMenuId() {
        return menuId;
    }

    public void setMenuId(String menuId) {
        this.menuId = menuId;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public String getCuisine() {
        return cuisine;
    }

    public void setCuisine(String cuisine) {
        this.cuisine = cuisine;
    }

    public boolean getIsAvailable() {
        return isAvailable;
    }

    public void setIsAvailable(boolean isAvailable) {
        this.isAvailable = isAvailable;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getRestaurantId() {
        return restaurantId;
    }

    public void setRestaurantId(String restaurantId) {
        this.restaurantId = restaurantId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getCuisineType() {
        return cuisineType;
    }

    public void setCuisineType(String cuisineType) {
        this.cuisineType = cuisineType;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public BigDecimal getPreviousPrice() {
        return previousPrice;
    }

    public void setPreviousPrice(BigDecimal previousPrice) {
        this.previousPrice = previousPrice;
    }

    public boolean isVegetarian() {
        return vegetarian;
    }

    public void setVegetarian(boolean vegetarian) {
        this.vegetarian = vegetarian;
    }

    public boolean isVegan() {
        return vegan;
    }

    public void setVegan(boolean vegan) {
        this.vegan = vegan;
    }

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }

    public int getPreparationTimeMins() {
        return preparationTimeMins;
    }

    public void setPreparationTimeMins(int preparationTimeMins) {
        this.preparationTimeMins = preparationTimeMins;
    }

    public float getRating() {
        return rating;
    }

    public void setRating(float rating) {
        this.rating = rating;
    }

    public int getNumRatings() {
        return numRatings;
    }

    public void setNumRatings(int numRatings) {
        this.numRatings = numRatings;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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
        return Instant.ofEpochMilli(lastUpdated > 0 ? lastUpdated : updatedAt);
    }

    /**
     * Returns true when a price change is detected (previousPrice differs from price).
     */
    public boolean hasPriceChanged() {
        return previousPrice != null && price != null
                && previousPrice.compareTo(price) != 0;
    }

    /**
     * Returns the percentage change in price. Positive means price increased.
     * Returns 0 if previous price is null or zero.
     */
    public double getPriceChangePercent() {
        if (previousPrice == null || price == null
                || previousPrice.compareTo(BigDecimal.ZERO) == 0) {
            return 0.0;
        }
        return price.subtract(previousPrice)
                .divide(previousPrice, 4, java.math.RoundingMode.HALF_UP)
                .doubleValue() * 100.0;
    }

    /**
     * Returns true if the item just became unavailable (availability changed to false).
     */
    public boolean becameUnavailable() {
        return eventType == EventType.MENU_ITEM_AVAILABILITY_CHANGED && !isAvailable;
    }

    // -----------------------------------------------------------------------
    // equals / hashCode / toString
    // -----------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MenuEvent that = (MenuEvent) o;
        String thisId = menuId != null ? menuId : itemId;
        String thatId = that.menuId != null ? that.menuId : that.itemId;
        long thisTime = lastUpdated > 0 ? lastUpdated : updatedAt;
        long thatTime = that.lastUpdated > 0 ? that.lastUpdated : that.updatedAt;
        return Objects.equals(thisId, thatId) && thisTime == thatTime;
    }

    @Override
    public int hashCode() {
        String id = menuId != null ? menuId : itemId;
        long time = lastUpdated > 0 ? lastUpdated : updatedAt;
        return Objects.hash(id, time);
    }

    @Override
    public String toString() {
        return "MenuEvent{" +
                "menuId='" + (menuId != null ? menuId : itemId) + '\'' +
                ", restaurantId='" + restaurantId + '\'' +
                ", itemName='" + (itemName != null ? itemName : name) + '\'' +
                ", category='" + category + '\'' +
                ", cuisine='" + (cuisine != null ? cuisine : cuisineType) + '\'' +
                ", price=" + price +
                ", isAvailable=" + (isAvailable || available) +
                ", eventType=" + eventType +
                ", lastUpdated=" + (lastUpdated > 0 ? lastUpdated : updatedAt) +
                '}';
    }
}

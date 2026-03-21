package com.zomato.pipeline1.events;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * POJO representing a menu event consumed from Kafka.
 * Captures changes to restaurant menu items for CEP processing.
 */
public class MenuEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String itemId;
    private String restaurantId;
    private String name;
    private String category;
    private String cuisineType;
    private BigDecimal price;
    private BigDecimal previousPrice;
    private boolean vegetarian;
    private boolean vegan;
    private boolean available;
    private int preparationTimeMins;
    private float rating;
    private int numRatings;
    private String imageUrl;
    private long createdAt;
    private long updatedAt;
    private String opType;

    public MenuEvent() {
    }

    public MenuEvent(String itemId, String restaurantId, String name,
                     String category, BigDecimal price, boolean available,
                     long updatedAt) {
        this.itemId = itemId;
        this.restaurantId = restaurantId;
        this.name = name;
        this.category = category;
        this.price = price;
        this.available = available;
        this.updatedAt = updatedAt;
    }

    // -----------------------------------------------------------------------
    // Getters and setters
    // -----------------------------------------------------------------------

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

    public Instant getEventTime() {
        return Instant.ofEpochMilli(updatedAt);
    }

    /**
     * Returns true when a price change is detected (previousPrice differs from price).
     */
    public boolean hasPriceChanged() {
        return previousPrice != null && price != null
                && previousPrice.compareTo(price) != 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MenuEvent that = (MenuEvent) o;
        return Objects.equals(itemId, that.itemId)
                && Objects.equals(updatedAt, that.updatedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, updatedAt);
    }

    @Override
    public String toString() {
        return "MenuEvent{" +
                "itemId='" + itemId + '\'' +
                ", restaurantId='" + restaurantId + '\'' +
                ", name='" + name + '\'' +
                ", price=" + price +
                ", available=" + available +
                ", updatedAt=" + updatedAt +
                '}';
    }
}

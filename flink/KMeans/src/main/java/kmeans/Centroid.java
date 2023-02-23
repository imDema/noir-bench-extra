package kmeans;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Objects;

public class Centroid implements Serializable, Comparable {
    private double x;                   // x coordinate
    private double y;                   // y coordinate
    private long n;                     // number of associated points
    private int id;                     // identifier of the centroid

    /**
     * Constructor method
     * @param x is the coordinate x
     * @param y is the coordinate y
     * @param id is the identifier of the centroid
     */
    public Centroid(double x, double y, int id) {
        this.x = x;
        this.y = y;
        this.n = 1;
        this.id = id;
    }

    public Centroid() {

    }

    /**
     * Set the x coordinate
     * @param x is the coordinate to set
     */
    public void setX(double x) {
        this.x = x;
    }

    /**
     * Set the y coordinate
     * @param y is the coordinate to set
     */
    public void setY(double y) {
        this.y = y;
    }

    /**
     * Get the x coordinate
     * @return the x coordinate
     */
    public double getX() {
        return x;
    }

    /**
     * Get the y coordinate
     * @return the y coordinate
     */
    public double getY() {
        return y;
    }

    /**
     * Get the identifier of the centroid
     * @return the identifier of the centroid
     */
    public int getId() {
        return id;
    }

    /**
     * Set the number of associated points to the centroid
     * @param n is the number of associated points to the centroid
     */
    public void setN(long n) {
        this.n = n;
    }

    /**
     * Get the number of associated points to the centroid
     * @return the number of associated points to the centroid
     */
    public long getN() {
        return n;
    }

    /**
     * @Override the toString method
     * @return the string representation of the object
     */
    @Override
    public String toString() {
        DecimalFormat df = new DecimalFormat("0.0000");
        return df.format(getX()) + "," +
                df.format(getY());
    }

    /**
     * @Override the method to check if two objects are equals
     * @param o is the object to be compared
     * @return true if the objects are equal; false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Centroid centroid = (Centroid) o;
        return Double.compare(centroid.getX(), this.getX()) == 0 &&
                Double.compare(centroid.getY(), this.getY()) == 0 &&
                Double.compare(centroid.getN(), this.getN()) == 0;
    }


    /**
     * @Override the method to get the hash code of the object
     * @return the hash code of the object
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.getX(), this.getY(), this.getN());
    }

    /**
     * @Override the method to compare two objects
     * @param o is the object to be compared
     * @return  0 if the coordinates of the objects are equal;
     *         -1 if the x coordinate of the object on which the method is invoked is less than the other x coordinate
     *         +1 if the x coordinate of the object on which the method is invoked is greater than the other x coordinate
     */
    @Override
    public int compareTo(Object o) {
        Centroid c = (Centroid) o;
        if (c.getX() == this.getX() && c.getY() == this.getY()) {
            return 0;
        } else if (c.getX() > this.getX()) {
            return -1;
        } else {
            return 1;
        }
    }
}